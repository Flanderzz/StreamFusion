# Phase 2 design: the Arrow dirty region, DataFusion overlay reads, and range hydration

Status: DECISIONS RESOLVED (maintainer review 2026-07-26; see the resolutions section at the end).
A prerequisite rework was also decided in the same review: the bucket-per-key-group table layout
is being removed (fragmentation proportional to max-parallelism was judged too much overhead) in
favor of a small fixed bucket count with RocksDB-style clipping at recovery — Phase 2 builds on
the de-bucketed store.

Original draft (2026-07-25): Written after the state-backend
speed-up round (124.1 s → 9.85 s measured on the q4 A/B; see `docs/benchmarks.md`) so the design
intent survives sessions. Approved direction from maintainer discussion: dirty region as
arrival-ordered Arrow batches + deletion bitmaps, DataFusion for reads, per-batch min/max stats
for pruning. The measured caveat that shaped this doc: the CPU the dirty-region rework removes
(`ScalarValue` encode/decode) is **under 1%** of the current backend profile, so this is not a
throughput fix — it is the foundation the watermark/timer operators (the remaining coverage gap)
need, and it must be built as such.

## Why these are one project

Point-access operators never need to see uncommitted state as a *set* — a resident working-set
slot is simply authoritative for its key. Watermark/timer operators do: firing means "every
buffered row with `t <= watermark`", and that range result must reflect uncommitted adds and
deletes. A map of decoded slots cannot express that; a dirty region held as Arrow batches can, as
a query. So the dirty-region representation and the range read path stand or fall together, and
the first consumer (a Phase 2 operator) is what makes either worth building.

## The dirty region

Per store (or per bucket — decide at plan review):

- **Arrival-ordered Arrow batches.** Mutations append rows; nothing rewrites. Paimon's
  deduplicate merge engine resolves equal-PK rows by in-commit sequence number, and paimon-rust
  assigns sequence numbers in write-arrival order, so duplicates are legal as long as the barrier
  writes the batches to one writer in arrival order. Deletes are `-D` value-kind rows appended in
  order (writing `-D` for a never-committed key is harmless), not Paimon deletion-vector files —
  DV mode trades a write-side lookup for a merge-free read, the wrong trade for a state table,
  and would put a new compatibility surface between paimon-rust and the Java compactor.
- **A deletion bitmap per batch** marks superseded and retracted rows. Its two jobs: restore
  one-row-per-PK-per-commit *as an optimization* (filter each batch by its bitmap at the barrier,
  vectorized, before writing), and keep range queries from returning superseded versions without
  sorting.
- **A key → row-ref index** (`ByteKey -> (batch, row)` of the live version) maintained on append.
  Point reads and bitmap maintenance are O(1) through it; it is also the "touched keys" set the
  overlay's anti-join needs.
- **Per-batch min/max** on the range column(s), maintained on append (or on batch seal). This is
  the `PruningStatistics` implementation: DataFusion's `PruningPredicate` evaluates a filter
  against container-level min/max exactly as it does for parquet row groups, and arrival-ordered
  batches are naturally rowtime-clustered, the same property that makes checkpoint-batched
  parquet runs prune well.

RocksDB cross-check: this is the memtable, reshaped. RocksDB tolerates duplicate keys in the
memtable via per-write sequence numbers and resolves at read/flush; Flink disables the WAL so the
memtable's durability *is* the checkpoint. Our arrival-order sequences play the seqno role, the
bitmap plays the role of memtable garbage that RocksDB's flush drops, and the barrier commit is
the flush. The wontdo on block state (48) rejected this shape *for the memory backend* because
the decode cost it removes was already below measurability there; the costs it removes here
(`ScalarValue` round trips, and the impossibility of range reads over decoded slots) are
different in kind — reconcile in `divergences/` when this ships.

## The read overlay

One shape serves points and ranges:

```
result(pred) = (committed_scan(pred) ANTI JOIN dirty_touched_keys ON key)
               UNION ALL
               dirty_scan(pred)        -- live versions only (bitmap applied)
```

- `committed_scan` runs against the **pinned** snapshot through paimon-rust — either directly (as
  `scan_buckets` does today) or via `PaimonTableProvider` (paimon-rust ships one; its workspace
  pins arrow 58 / DataFusion 54, exactly ours, and it takes a `Table` handle, so pinning is
  preserved). The provider brings filter/projection/limit pushdown with exact-vs-residual
  classification for free.
- `dirty_scan` is a `MemTable`-style provider over the dirty batches with the bitmap as a
  selection and `PruningStatistics` from the per-batch min/max.
- The anti-join keys come from the row-ref index — no scan needed to enumerate them.
- Point-access stores keep their current fast path (hydrate bucket → HashMap residency); the
  overlay is for range consumers. Do NOT route per-row point probes through a DataFusion plan —
  the group aggregate folds per input row and a plan per row is absurd; the working set remains
  the hot-loop surface.

RocksDB cross-check: Flink serves time-shaped firing from ordered iteration — timers in a
dedicated column family iterated by (time, key), buffered rows in `MapState<Long, List<row>>`
iterated per key. The overlay is the LSM equivalent: a merged view of memtable + SSTs under a
range predicate, with file stats standing in for the CF's total order. We do not need a total
order — firing collects *all* rows `<= watermark`, so pruning beats sorting.

## First consumer: rowtime keep-first dedup (Phase 2 rung 1)

Chosen because it has the simplest time-shaped state: a pending buffer of first-seen rows waiting
for the watermark, plus keep-first semantics per key.

- State remodel: pending rows become keyed rows `[kg, key, rowtime, payload...]` in one Paimon
  table (Flink's own shape for this is `MapState`-per-key with timers; ours collapses the timer
  into the row's own `rowtime` column). Keep-first means the *smallest-rowtime* row per key wins:
  the deduplicate merge engine keeps the *latest* by sequence, so the operator only appends a
  key's row when it improves on the current winner (read-your-writes through the index/working
  set), which preserves one-live-row-per-key without a custom merge engine.
- Watermark fire: overlay range query `rowtime <= wm` over the operator's key-group range; emit;
  append `-D` rows for the emitted keys (they leave pending state); commit at the next barrier as
  usual. Rows arriving after their key already fired are dropped by the keep-first check exactly
  as the memory path does.
- Restore: the pending table restores through the existing incremental checkpoint machinery
  unchanged; the watermark itself is Flink-managed (re-delivered after restore).
- Timer parity: no separate timer state — the range query *is* the timer sweep. RocksDB
  cross-check: Flink's `PriorityQueueStateType.ROCKSDB` keeps timers in an ordered CF precisely
  because iterating a billion timers from heap is impossible; our pruning-scan plays that role,
  and the timer deadline that WindowRank/interval joins keep in raw state today folds into the
  table (their rungs come later: WindowRank, OVER, interval/window joins, in that order).

## What this is measured by

Not q4. The A/B for Phase 2 rung 1 is a rowtime-dedup query (q18-shaped with event-time order)
under the Paimon backend vs memory state, plus parity against Flink. The dirty-region write path
can additionally be A/B'd on the row-payload operators (dedup/normalizer/join), where state rows
are input rows and batch-slice retention can replace the per-cell encode — but only claim what a
cool-machine run shows; the 2026-07-25 session's thermal drift (memory baseline 4.4–8.7 s)
swallowed effects under ~20%.

## Decisions (maintainer review, 2026-07-26)

1. **Dirty region per store.** Moot at fine grain now that the bucket count is small and
   decoupled from max-parallelism.
2. **DataFusion merges the two worlds.** The committed side reads through a DataFusion table
   provider over the pinned table; the dirty region is a second provider (deletion bitmaps as
   selection, per-batch min/max as `PruningStatistics`); the overlay is a DataFusion plan.
3. **The write path stays as it is.** No DataFusion on writes — there is no relational work in
   "encode dirty state and hand it to the writer" for a query engine to improve; it would add
   plan overhead for nothing. Batch-slice retention for row-payload codecs remains a possible
   later change, orthogonal to DataFusion and currently unjustified by profile (<1%).
4. **One DataFusion query answers a range read** over the in-memory write buffer plus the
   committed Paimon data — that single overlay query IS the coherence mechanism. The hot loop's
   per-row point checks do not run a plan per row: they hit the dirty region's key→row-ref index
   directly, which is a view over the same batches the query scans, so points and ranges can
   never disagree.
