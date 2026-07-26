# Persistent state: local Paimon tables, not RocksDB and not Arroyo's model

**Kind:** architectural — where durable operator state lives and how it checkpoints.
**Diverges from:** Arroyo (and the obvious RocksDB-via-Rust alternative).
**Forced by parity:** partly — Flink's incremental-checkpoint contract shapes the design; the
storage engine choice is ours.

## Their decision

Arroyo keeps operator state in memory and checkpoints it to object storage through its own
controller; there is no per-operator embedded KV store and no Flink-style shared-state registry.
DataFusion Comet is stateless (batch), so it has no position here. Flink's own persistent backend
is RocksDB behind JNI: rowwise, byte-serialized keys *and* values, per-entry
serialize/deserialize on every access, incremental checkpoints via immutable SST files registered
with the `SharedStateRegistry`.

## What we do instead

Native operator state moves into a **local Apache Paimon primary-key table** (via paimon-rust,
Vortex file format) behind a storage seam in the Rust operators — selected with Flink's normal
`state.backend.type` toggle, memory remaining the default. The store holds exactly **two
components: a write buffer and the disk table**. Reads resolve per input batch with one
point-read join: the batch's keys not already in the write buffer are pushed into the table
reader as an exact `IN` predicate (file/page stats prune, then a single hash-set pass filters
rows at parquet decode — a fork patch, upstreamable, replaced the reader's per-literal `IN` loop
that would have made this quadratic), and the matched rows live only until the end of the
batch's bundle. There is **no retained cache of clean rows between bundles**: re-reads are
served by the OS page cache plus decode, never by a second in-memory copy of committed state.
The split list is planned once per pinned snapshot (the table is immutable between barriers), so
per-batch probes pay no manifest walk. Two earlier read designs were tried and rejected: the
original key-probe-per-batch on the bucket-per-key-group layout profiled as a file-open storm
(evidence that turned out to be about the layout, not the probe granularity), and the
interval-resident working set that replaced it duplicated committed state in memory and tied the
memory bound to touched-state size rather than written-state size. Writes buffer as dirty
working-set entries and commit as one typed Arrow batch per checkpoint barrier. Durability lands
exactly at checkpoints — between barriers the write buffer is RAM, playing the role RocksDB's
memtable+WAL play, except the "WAL" is the checkpoint itself.

Why Paimon over rust-rocksdb:

- **No per-entry serialization tax.** State rows are typed Arrow columns end to end — the write
  path is `write_arrow_batch`, the read path streams Arrow — where any KV engine forces
  encode/decode per entry per access.
- **Incremental checkpoints are structural.** A Paimon snapshot is a manifest-pinned set of
  immutable, uniquely named files; "new since the last checkpoint" is a manifest diff. The Java
  side mirrors `RocksIncrementalSnapshotStrategy`'s bookkeeping (confirmed-base placeholders,
  notification-delay pruning, sharing-strategy switch) over Paimon files and emits ordinary
  `IncrementalRemoteKeyedStateHandle`s, so the JM-side registry contract is Flink's own.
- **A small fixed bucket count, clipped at recovery — the RocksDB shape.** The table carries a
  computed key-group INT column as leading primary-key column and bucket key under Paimon's `mod`
  bucket function, but the bucket count is deliberately small and decoupled from max parallelism
  (`streamfusion.state.paimon.buckets`, default 1: one LSM per subtask). The original design set
  `bucket = maxParallelism` so bucket id equaled key group and rescale was free file reassignment
  — but that wrote one file per touched key group per commit, fragmentation proportional to max
  parallelism, judged too much steady-state overhead for a property rescale rarely uses (Flink
  itself never physically partitions RocksDB by key group; the group is a key prefix in one CF,
  and rescale clips). Key-group locality survives de-bucketing because `kg` leads the primary
  key: files' row groups are kg-clustered, so the per-batch key probe pushes the keys' groups as
  a stats-prunable companion predicate and reads stay proportional to touched groups. Restore has two paths: a single source covering exactly
  this subtask's range (and the same bucket count) adopts every bucket wholesale — data files
  hard-linked, committed by existing metadata (public `CommitMessage`), no row read — while
  rescale (or a bucket-count change) pays a one-time clip at recovery: each source is scanned
  under a key-group-range predicate and the surviving rows are rewritten into the fresh table in
  one commit, RocksDB's restore-time clip in Paimon terms.
- **The same tables on object-store FileIO later** are the disaggregated backend with no redesign.

## Costs and edges we accept

- paimon-rust has **no LSM compaction or snapshot expiry** yet, and we deliberately carry **no
  native compaction of our own**: table maintenance belongs exclusively to the optional
  `streamfusion-paimon-compactor` module, which hands the whole operation to **stock Java
  Paimon** (its own picks, its sequence-preserving rewriter, its exact deletion
  handling), running on a **background thread per operator backend** kicked after each barrier's
  commit — the RocksDB model, where compaction never runs on the write path (running it
  synchronously at the barrier measured slower than no maintenance at all). Maintenance commits
  race the barrier's data commits safely under Paimon's optimistic commit retry on both sides,
  and the local GC only deletes files it previously listed as live, so an in-flight round can
  lose an input to GC and retry, never corrupt.
  Cross-implementation round trips (Rust writes → Java reads and compacts → Rust
  restores and continues) are pinned by the module's tests against released Paimon. Without the
  module, tables stay correct but accumulate one sorted run per touched bucket per checkpoint
  (warned, not failed) — one maintenance implementation, zero drift, was judged worth that
  degradation. (A native port of Java's `UniversalCompaction` picks was built and then removed
  by that decision — commit b555abf holds it if the trade ever reverses; upstreaming real
  compaction to paimon-rust is the durable fix.) Local files unreachable from the latest
  snapshot are unlinked after each checkpoint (uploads read from per-checkpoint hard-link
  directories, so GC and uploads never race).
- Vortex state files are **not readable by released Java Paimon** — the Java Vortex format
  (reader and writer over the native vortex library) exists on Paimon master, targeted at 2.0,
  and is absent from every 1.4.x release. State files therefore default to `parquet` (Java can
  maintain and inspect them today); `vortex` is opt-in and currently unmaintained. Values stay
  Rust-defined either way.
- Canonical savepoints cannot be expressed; native-format savepoints work.
- Multiset-state aggregates (retracting MIN/MAX, DISTINCT) stay on memory state until the row
  codec grows side tables (see `docs/coverage-and-fallbacks.md` §c).

## State shapes mirror Flink's state primitives

The store grew four shapes, each the analog of a Flink state primitive as RocksDB lays it out:
a **single-value** store (ValueState; PK `[kg, k]`, one typed row per key), a **list** store
(ListState; PK `[kg, k, ord]`, one row per element, a dirty key rewriting its whole list — exactly
RocksDB ListState's whole-value rewrite — with positions preserving order-sensitive semantics like
Top-N tie order), and a **map** store (MapState; PK `[kg, k, r]`, one row per entry, `r` the row's
Flink BinaryRow bytes — a stable wire format, unlike arrow-row). The updating join runs two map
tables (one per side) under one operator backend — the analog of Flink's two named join states as
two column families in one RocksDB — carried by one incremental handle whose meta document stores
an opaque snapshot token the native store packs both snapshot ids into. The map store's flush is
per-entry, like RocksDB MapState's per-entry puts and deletes, but derived rather than tracked:
the operator mutates a key's whole entry map in place, and at the barrier the store diffs it
against the image read from the table when the key was first fetched — only entries that differ
are upserted, only vanished rows are tombstoned, so a hot join key's untouched rows cost nothing
per checkpoint.

The fourth shape serves the watermark-driven operators (first consumer: rowtime keep-first
dedup): a **time-buffered** store whose write buffer is not a decoded map but arrival-ordered
Arrow batches with a per-batch liveness bitmap, a key index, and per-batch min/max on the time
column — a queryable set, because watermark firing must answer "every pending row with
`rowtime ≤ watermark`" *including* uncommitted adds and deletes, which per-key slots cannot
express. The firing read is an overlay: the committed table scanned under the time predicate
(stats-pruned, exact at decode), minus rows shadowed by an uncommitted version of the same key (a
DataFusion right-anti hash join against the buffer's touched keys), plus the buffer's own live
rows in range. RocksDB cross-check: Flink serves the same firing from ordered iteration (timers
in a dedicated CF iterated by time); the pruned range scan plays that role — no total order is
needed because a firing collects *all* rows ≤ watermark. Payload moves as Arrow columns end to
end in this shape (input batch → buffer → barrier flush; committed scan → emission), never
through per-cell scalars, and a fired key keeps a marker row on disk so emitted-ness survives
checkpoints where the memory path grows an in-RAM emitted-key set forever.

The second range-read consumer is **event-time window rank** (window Top-N / window dedup), on
the same shape with a composite key: one table row per buffered rank position under
`[kg, key, window_end, window_start, ord]`. Its open windows' buffers stay decoded in memory for
the checkpoint interval — every touch re-ranks them, so they are the write buffer, not a cache —
and stage into the dirty region at the barrier as whole-buffer rewrites (upserts `0..len`,
tombstones for vacated committed positions), the RocksDB `ListState` rewrite shape. A window
first touched in an interval seeds from the committed table *before* the batch's own rows rank
in, preserving the ROW_NUMBER arrival-order tie-break. Firing merges the in-memory buffers with
a committed scan under `window_end ≤ watermark` (positions already fired this interval are
shadowed by the region's staged deletions), then stages `-D` rows for every fired position. Two
deliberate scope edges: the watermark rides the opaque snapshot token (the memory path persists
it in its raw snapshot; without it a restored subtask re-buffers replayed rows of already-fired
windows), and the **proctime** window rank keeps memory state — it closes windows on
processing-time timers whose deadline travels in raw state, not on watermarks.

The full design record, including the verified paimon-rust API survey and the rejected
alternatives (rust-rocksdb baseline, Tonbo, fjall, SlateDB, ForSt), is in
`.claude/research/paimon-vortex-state-backend-plan.md`.
