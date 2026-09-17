# LIMIT

**Status:** Native — `LIMIT`/`FETCH`, with or without `OFFSET`, over insert-only or updating input.

Flink lowers a `LIMIT`/`FETCH` clause to a rank filter and reuses the same rank operator family as
[Top-N](top-n.md) — a plain `LIMIT n` is nothing more than a rank filter with a constant range
starting at 1. Everything the Top-N page describes about ranker selection and idle-state TTL
applies here unchanged, since it *is* the same operator underneath.

An insert-only `OFFSET` uses the full-buffer retracting ranker. An update-fast `OFFSET` uses
unique-key replacement state, retaining ranks 1 through `offset + fetch` and emitting only
the selected range. The hidden prefix must survive checkpoints because later updates can move
its rows into view.

## Updating input

Global `ORDER BY … LIMIT n` uses the strategy Flink selected during changelog inference.
A monotonic aggregate such as `GROUP BY k ORDER BY COUNT(*) DESC, k ASC LIMIT 100` uses the
update-fast ranker and replaces prior versions by the planner's unique key. Recomputing the
strategy after substitution would be unsafe: that input may already omit UPDATE_BEFORE rows.
General retracting aggregates and unordered updating `LIMIT` use the full-buffer retracting
ranker, which can promote a row beyond the selected range when a current row is deleted.

Update-fast OFFSET matches Flink's ordered positional UPDATE_BEFORE/UPDATE_AFTER cascades,
including when an updated key moves into the hidden prefix and when a restored visible row
receives an identical update. Tests cover projected and hidden rank, tied sort keys, NULL keys,
offsets beyond the available rows, and memory/RocksDB restore of the hidden prefix.

The grouped pipeline stays columnar across the singleton exchange. Tests compare raw row kinds
with mini-batching disabled, including NULL ordering, decrements and complete group deletion.
Mini-batch tests compare final materializations with size-four bundles and end-of-input
flushes. The SQL fixture keeps processing-time batch markers at zero for both executions:
crossing a wall-clock boundary can otherwise split identical rows into different bundles,
changing the host's retained row kinds and even its final OFFSET result. The fallback case
still executes both jobs and requires zero native substitutions and the precise reason.
Hidden-rank retracting OFFSET always preserves
per-record cascades: emitting a row mutates its retained kind, affecting later full-row equality.
Its sort-key counts advance independently of successful payload removals, exactly as in Flink's
heap state backend. Checkpoints and memory/RocksDB transitions preserve both kinds and counts. The
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

The same workload with `LIMIT 100 OFFSET 1`, using the same release build and measurement
method, gave medians of **0.400610s Flink / 0.785646s native (0.510x)**. This also expands verified
native composition without a standalone throughput gain. Reproduce with
`SF_BENCHMARK=true mvn -Pbench -pl :streamfusion-runtime -am test
-Dtest=UpdatingLimitBenchmark -Dlimit.offset=1 -Dsurefire.failIfNoSpecifiedTests=false`.
Omit `-Dlimit.offset=1` to measure the zero-offset case.

The retracting variant (`SUM(v)` with alternating positive and negative contributions) uses
20,000 rows, 64 keys and `LIMIT 100 OFFSET 1`. On the same Apple M4 Pro / JDK 17 release setup,
two warmups and five interleaved runs gave **0.182802s Flink / 0.455513s native (0.401x)**.
This short end-to-end workload includes job setup, both transposes and the rowwise sink; it is
coverage and composition work, not a standalone throughput improvement. Reproduce with
`SF_BENCHMARK=true mvn -Pbench -pl :streamfusion-runtime -am test
-Dtest=UpdatingLimitBenchmark -Dlimit.retract=true -Dlimit.rows=20000 -Dlimit.keys=64
-Dlimit.offset=1 -Dsurefire.failIfNoSpecifiedTests=false`.

## Gap

- Hidden-rank retracting OFFSET after an upstream mini-batch aggregate, rank or other operator
  that can change intermediate changelog order. Source changelogs through projections, filters,
  exchanges and batch markers remain native. Preserving the upstream bundle order is the
  remaining composition work in [#102](https://github.com/datafusion-contrib/StreamFusion/issues/102).

- MAP/MULTISET fields in retained rows, including nested ARRAY/ROW fields, because the shared
  Top-N row codec cannot store them. These queries fall back before operator initialization.
- A `LIMIT`/`OFFSET` with no `FETCH` (row count) at all — an unbounded skip.
- A SortLimit whose selected Flink rank strategy cannot be read or has not been resolved.
