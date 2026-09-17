# Incremental accumulator merge for session (and sliding) windows

**Kind:** algorithmic — internal implementation differs; output is identical.
**Diverges from:** Arroyo.
**Forced by parity:** no — this is a chosen optimization, not a requirement.

## Their decision
Arroyo's session and sliding aggregating windows **store raw input batches** in a
tiered structure (e.g. `batches_by_start_time`) and **re-aggregate** the relevant
rows whenever a window advances or sessions merge. The raw rows are the source of
truth; aggregation is recomputed from them.

## What we do instead — which is what Flink itself does
We keep, per key, only the **incremental accumulators** for each open window, and
when two windows merge we fold one accumulator's partial state into the other via
`merge_batch`. Raw rows are discarded after they are folded in.

This is *not* a freelance optimization: it mirrors Flink's own window operator.
Flink's combiner (`AggCombiner` / `RecordsWindowBuffer`) buffers raw records **in
memory only**, folds them into one accumulator per window, and persists **only the
accumulator** to state — two-phase merges slice accumulators, session merges
window accumulators. So for mergeable aggregates we match the host's strategy
exactly; **only Arroyo differs**, by retaining raw batches and re-aggregating.

## Window distinct state

COUNT(DISTINCT) uses DataFusion's distinct count accumulator within the same window lifetime.
Its mergeable partial is the set of values as an Arrow list, rather than a scalar count. The
local/exchange/global pipeline unions these lists and snapshots the resulting set. This follows
Arroyo's DataFusion partial/final aggregation boundary (`tumbling_aggregating_window.rs` and
the planner's aggregate extension) while retaining our existing per-window accumulator layout.
Flink's local MapView fields are replaced by the list partials, so both native stages share
one explicit intermediate schema. Variable-sized sets currently use the existing snapshot
fallback on RocksDB; they do not enter its fixed-field accumulator row codec.

## Late local slices

Arroyo's sliding operator bins batches and skips a bin preceding its current watermark bin.
That admission rule is narrower than Flink's overlapping-window contract: a slice can have
fired while a larger window containing it remains open. Flink's local slicing aggregate
accepts the row; its final processor checks the last containing window and updates only
unfired windows. Our local Arrow partials likewise defer late-data admission to the final
merge. The local update path is explicit, so a restored local watermark cannot suppress a
still-useful slice. The global watermark remains checkpointed and prevents reopening a
completed window. Arrow import/release and exception ordering retain the existing bridge
pattern checked against Comet's JNI Arrow import path.

RisingWave's `HopWindowExecutor` is a related reference, but expands each input into window
rows and transforms window-column watermarks; its hash aggregate owns final emission and
state cleanup. It does not supply a matching local-slice implementation to transplant here.
We keep the existing partial-accumulator architecture and Flink's late-window semantics.

## Why
Every aggregate we support (`SUM`/`MIN`/`MAX`/`COUNT`, and integer `AVG`) has
associative, commutative, mergeable partial state, so merging accumulators yields
exactly the same answer as re-aggregating the union of rows — but in bounded
memory (state per open window, not per row) and without recomputation on merge.

The result is identical to Flink (and to Arroyo), verified by the parity harness,
including the case where a late, out-of-order element bridges two open sessions.

## Why diverge at all, given the "copy them" default
Because the divergence is only from Arroyo, and we land on the *host's* approach:
matching Flink is the prime directive, and Flink keeps accumulators, not rows.
Arroyo's raw-retain model exists partly to support aggregates whose state is not
cleanly mergeable — a generality we do not need yet.

**Future (non-mergeable aggregates).** When we add aggregates that are not
associative/invertible, the precedent in *both* engines is **not** a "partial
accumulator + all rows" dual store — that redundancy is what they avoid. Instead
the state is chosen per aggregate: mergeable ones keep the small accumulator
(above); non-mergeable ones keep a **value→count multiset** and recompute on
demand (Flink's `MaxWithRetractAccumulator` is a running scalar plus a
`MapView<value, count>`; Arroyo's `IncrementalState::Batch` is a `HashMap<value,
count>`). We would follow that — per-aggregate state selection, value-multiset for
the non-mergeable set — rather than retaining raw rows alongside partials.
