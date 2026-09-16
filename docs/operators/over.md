# OVER

**Status:** Native across all four frame shapes, with the gaps enumerated below.

`OVER` runs over one ascending order (rowtime or, where noted, proctime) and one window group. Each
aggregate reads its own — possibly different — value column of type
bigint/int/smallint/tinyint/double/float (narrow ints and 4-byte float keep the host's narrow result
type rather than being widened). `FIRST_VALUE`/`LAST_VALUE` and the window functions
`ROW_NUMBER`/`RANK`/`DENSE_RANK` (no value column, unbounded frame) are admitted alongside the
aggregates below.

Constant NULL arguments are also supported with their declared type, including
`MIN(CAST(NULL AS VARCHAR))`, `MAX(CAST(NULL AS VARCHAR))`, and `COUNT(CAST(NULL AS VARCHAR))`.
They can share a frame with numeric aggregates and `COUNT(*)`; typed NULL Arrow columns are
handled without attempting a numeric conversion.
Running MIN/MAX checkpoints preserve this typed NULL state alongside other aggregates.

Event-time parity fixtures keep the watermark behind the entire input timestamp range until
end of input, so file enumeration order cannot introduce accidental late rows. Late-row tests
control the input order separately.

## Frame shapes

### Unbounded `RANGE … CURRENT ROW` and `ROWS … CURRENT ROW` (running folds)

Both retain incremental aggregates per partition across batches. Event-time RANGE folds all
rows tied at a timestamp before emitting their shared result. ROWS emits after each row,
including separate running results for tied rows in their arrival order. Watermarks release
buffered event-time rows in timestamp order; proctime folds eagerly in arrival order.

`COUNT(*)` counts every row, while `COUNT(value)` skips NULL values. The planner materializes
one non-null BIGINT constant for row counting and reuses the existing aggregate kernel, frame
state, TTL and checkpoint paths. This works for all four admitted frame shapes, including
mixed aggregates, nullable partition keys and unpartitioned input.

### Bounded `ROWS BETWEEN n PRECEDING AND CURRENT ROW`

Recomputed over the row slice — a fixed count of preceding rows plus the current one.

### Bounded `RANGE BETWEEN INTERVAL n PRECEDING AND CURRENT ROW`

Recomputed over the rowtime interval — every row within `n` of the current row's rowtime.

## Late event-time rows

Unbounded frames admit only timestamps strictly greater than the current watermark.
Bounded ROWS and RANGE frames instead compare against that partition's last-fired timestamp,
initially zero. Equality is late; an untouched partition can accept a positive timestamp behind
the global watermark. Rows tied before their timer fires remain admissible together.

The newest retained frame row preserves the last-fired timestamp through memory snapshots,
RocksDB checkpoints, and backend transitions. A zero-width ROWS frame keeps one marker row,
which is excluded from the next row's aggregate. RANGE eviction uses each partition's own
last timestamp. Its event-time cleanup timer clears the whole partition and resets admission:
Flink's deadline hysteresis registers `timestamp + floor(1.5 * interval) + 1` when the existing
deadline precedes `timestamp + interval + 1`. Those deadlines are checkpointed alongside frames;
pending-only partitions reconstruct them from their buffered arrivals on canonical restore.

## Proctime order

The running and bounded-ROWS frames are native on proctime as well: arrival order, eager emit, no
wall-clock timer needed.

A **bounded-RANGE frame over proctime** falls back — with processing time materialized as a fixed
per-batch timestamp, a wall-clock-interval frame has no meaningful definition.

## Gaps

The matcher declines:

- Direct `AVG` calls. Flink can lower some SQL AVG forms to supported SUM/COUNT aggregates.
- A decimal or other non-numeric value column, except constant NULL arguments.
- A `PARTITION BY` key outside bigint/int/string/boolean/date/timestamp/decimal.
- A frame not of the form `… PRECEDING .. CURRENT ROW` (a `ROWS`/`RANGE` lower bound that isn't a
  constant preceding offset or UNBOUNDED PRECEDING).
- A bounded-RANGE frame over a proctime order.

## Parity, not gaps

Flink itself rejects or single-groups these in streaming, so not running them natively matches Flink
rather than falling short of it: more than one window group, decimal bounded frames, `FOLLOWING`
frames, non-time or descending order, and `LAG`/`LEAD`.

## Idle-state TTL

`OVER` runs `table.exec.state.ttl` natively across all four frame shapes, but the mechanics differ
by shape:

- **Rowtime frames and the proctime bounded-ROWS frame** share a per-key cleanup deadline (the same
  scheme as the temporal join): registered on every element, with hysteresis and a
  `minRetentionTime > 1` enablement threshold, checked lazily and swept, clearing the key's
  accumulator and frame buffer silently. One wrinkle: at the deadline, the rowtime shapes *defer*
  while the key still has buffered rows the watermark hasn't folded (the timer re-registers and
  waits), whereas the proctime bounded-ROWS frame clears its retract frame unconditionally — so that
  frame can observably restart short.
- **The proctime unbounded fold** instead puts a per-value TTL (`> 0` enables, refreshed on last
  write) directly on its accumulator. An expired key visibly restarts its running fold — and its
  `ROW_NUMBER`/`RANK` numbering — from zero, exactly as Flink's `NeverReturnExpired` state does.
- **The bounded-RANGE rowtime frame** takes no retention at all: Flink's own function accepts none,
  since event-time frame eviction and cleanup bound state, so `table.exec.state.ttl` changes nothing there.

With that, nothing declines a nonzero retention setting. See [Configuration](../configuration.md) for
the TTL flag surface, and [window aggregate](window-aggregate.md) for the (unaffected — no idle-state
TTL applies) window operators.

## Running ROWS validation

The focused Flink 2.2.1 suite covers COUNT(*) versus nullable COUNT, mixed aggregates, constant
arguments, DISTINCT, empty/all-NULL input, multiple partitions, timestamp peers, late admission,
and 5,003-row multi-batch input. Operator tests cover TTL expiry and running-state continuation
across RocksDB checkpoints and memory/RocksDB backend transitions.

`RunningRowsBenchmark` measures proctime COUNT(*), COUNT(value), SUM, MIN and MAX over one
million rows and 64 keys, parallelism one, with both row/Arrow transposes and a row blackhole sink.
On an Apple M1 Max/JDK 17, the September 16, 2026 release run used two warmups and five interleaved
measurements: median Flink **0.605496 s**, native **0.591282 s** (**1.024x**). The difference is small;
this establishes comparable end-to-end performance for the new coverage, not a substantial
speedup. Keeping OVER columnar also lets it compose with adjacent native operators.

```bash
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench -Dtest=RunningRowsBenchmark
```
