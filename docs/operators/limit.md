# LIMIT

**Status:** Native — `LIMIT`/`FETCH` over insert-only or updating input. `OFFSET` is native over
insert-only input; updating OFFSET shapes remain on Flink.

Flink lowers a `LIMIT`/`FETCH` clause to a rank filter and reuses the same rank operator family as
[Top-N](top-n.md) — a plain `LIMIT n` is nothing more than a rank filter with a constant range
starting at 1. Everything the Top-N page describes about ranker selection and idle-state TTL
applies here unchanged, since it *is* the same operator underneath.

`OFFSET` is handled: it runs over the retracting ranker, applied to the (insert-only) input.

## Updating input

Global `ORDER BY … LIMIT n` uses the strategy Flink selected during changelog inference.
A monotonic aggregate such as `GROUP BY k ORDER BY COUNT(*) DESC, k ASC LIMIT 100` uses the
update-fast ranker and replaces prior versions by the planner's unique key. Recomputing the
strategy after substitution would be unsafe: that input may already omit UPDATE_BEFORE rows.
General retracting aggregates and unordered updating `LIMIT` use the full-buffer retracting
ranker, which can promote a row beyond the selected range when a current row is deleted.

The grouped pipeline stays columnar across the singleton exchange. Tests compare raw row kinds
with mini-batching disabled, including NULL ordering, decrements and complete group deletion.
Mini-batch tests compare final materializations under the existing net-diff contract. The
single-phase aggregate is used where a retracting two-phase SUM still has its own admission
gap; enabling LIMIT does not bypass an upstream aggregate's gate.

Checkpoint and memory/RocksDB savepoint tests cover the singleton partition for all three modes:
unique-key replacement, ordered retractions and arrival-ordered retractions. The general
retracting mode retains the full input buffer; the limit does not bound retained state.

## Idle-state TTL

Native here too, for the same reason: a `LIMIT` lowers to a rank, and that rank's TTL machinery
runs regardless of which SQL surface produced it. See [Top-N](top-n.md) for the ranker-specific
expiry granularity, [TTL semantics](index.md#idle-state-ttl) for the general rule, and
[Configuration](../configuration.md) for the flag surface.

## Updating LIMIT measurement

`UpdatingLimitBenchmark` measures 1 million rows grouped into 4096 keys, ordered by descending
COUNT and ascending key with LIMIT 100, at parallelism 1. A release/mimalloc build against Flink
2.2.1, two warmups and five interleaved runs gave medians of **0.428493s Flink / 0.805321s native
(0.532x)**. Both row/Arrow transposes and the rowwise blackhole sink remain in the measured path;
the plan asserts native aggregation and Top-N. This row-fed composition is substantially slower
than Flink. The change adds verified composition through the existing rankers, not a throughput
improvement; reducing the existing update-fast ranker's cost remains separate optimization work.

## Gap

- MAP/MULTISET fields in retained rows, including nested ARRAY/ROW fields, because the shared
  Top-N row codec cannot store them. These queries fall back before operator initialization.
- A `LIMIT`/`OFFSET` with no `FETCH` (row count) at all — an unbounded skip.
- Updating input paired with `OFFSET`. Flink's offset path emits positional UPDATE_BEFORE /
  UPDATE_AFTER cascades even without a projected rank; the native membership-diff path is not
  equivalent. Update-fast OFFSET also needs explicit replacement-state support. These remain
  tracked in [#102](https://github.com/datafusion-contrib/StreamFusion/issues/102).
- A SortLimit whose selected Flink rank strategy cannot be read or has not been resolved.
