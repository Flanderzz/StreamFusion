# Temporal functions

**Status: partial.** All Flink 2.2.1 temporal scalar families have expression implementations.
New timestamp-producing paths require the range opt-in below. Pattern time functions still
require the unsupported `MATCH_RECOGNIZE` operator.

## Function inventory

| Functions | Execution and admission |
|---|---|
| `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `LOCALTIME`, `LOCALTIMESTAMP`, `NOW()`, `CURRENT_ROW_TIMESTAMP()` | Rust clock functions; sampled during each batch evaluation, using the session zone for local fields. These nondeterministic functions are admitted by default. |
| `PROCTIME()`, internal `PROCTIME_MATERIALIZE` | Rust processing-time clock, evaluated at execution time. |
| `EXTRACT`, `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAYOFYEAR`, `DAYOFMONTH`, `DAYOFWEEK`, `HOUR`, `MINUTE`, `SECOND` | All forms Flink accepts for DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ and intervals. The existing DATE/plain-TIMESTAMP quarter/week/day kernels stay in Rust; other cases use Flink's generated expression code. |
| `TO_DATE(text)`, `TO_DATE(text, format)` | The one-argument parser stays in Rust; formatted parsing uses Flink. Dynamic formats, NULL behavior and invalid-input failures follow Flink. |
| `TO_TIMESTAMP(text[, format])` | Flink parsing, including dynamic formats. A timestamp result crossing into Arrow requires the range opt-in. |
| `TO_TIMESTAMP_LTZ` | Flink's numeric and string overloads, including format and explicit-zone arguments. Flink validates numeric precision (0 or 3). The existing integer-epoch, literal-precision-3 form remains admitted by default; other timestamp outputs require the range opt-in. |
| `DATE_FORMAT`, `UNIX_TIMESTAMP(text[, format])`, `FROM_UNIXTIME`, `CONVERT_TZ` | Flink evaluation supports dynamic patterns and zones. The existing literal numeric-pattern DATE_FORMAT fast path for plain timestamps remains in Rust. |
| `UNIX_TIMESTAMP()` | Rust current Unix-seconds clock. |
| `TIMESTAMPADD`, `TIMESTAMPDIFF` | Flink calendar and elapsed-time arithmetic, including its month-end and precision rules. New timestamp outputs require the range opt-in. |
| `FLOOR(timepoint TO unit)`, `CEIL`, `CEILING` | Flink rounding; plain TIMESTAMP DAY/HOUR/MINUTE/SECOND/MILLISECOND use a Rust kernel. Timestamp outputs require the range opt-in. Numeric FLOOR/CEIL admission is unchanged. |
| Temporal `CAST`, date/time/timestamp/interval literals, interval arithmetic, temporal comparisons, `OVERLAPS` | Flink-generated temporal subexpressions plus typed Arrow literals. New timestamp outputs require the range opt-in; existing timestamp precision widening and elapsed day-time interval arithmetic retain their admission. |
| `CURRENT_WATERMARK(time_attribute)` | Calc projections and predicates read the last watermark received by that operator; NULL before its first watermark. Join/UNNEST residuals without a Calc watermark context fall back. |
| `SOURCE_WATERMARK()` | Source declaration, not a scalar calculation. Existing DataStream/source watermarks are forwarded into the native pipeline. Native connector scan admission is unchanged. |
| `TUMBLE`, `HOP`, `CUMULATE`, `SESSION` | Existing window implementations and their shape/time-zone gates; see [window aggregate](window-aggregate.md) and [windowing TVF](window-aggregate.md#windowing-tvf-window-assignment). |
| `TUMBLE_START`, `TUMBLE_END`, `TUMBLE_ROWTIME`, `TUMBLE_PROCTIME`; corresponding `HOP_*` and `SESSION_*` helpers | Existing legacy window properties; see [window aggregate](window-aggregate.md). |
| `MATCH_ROWTIME`, `MATCH_PROCTIME` | Fall back with [MATCH_RECOGNIZE](unsupported.md). A scalar implementation cannot supply pattern-match context. |

Flink still rejects invalid signatures, invalid units and expressions outside their SQL context.
The inventory describes Flink's functions, not extra overloads supplied by other SQL dialects.

## Exact evaluation and expression fusion

Temporal functions use Flink 2.2.1's expression generator, compiled once when the native Calc is
planned. The generated evaluator travels in the existing serializable scalar-function binding,
opens on the task manager, and receives argument columns through the existing batched JVM upcall.
Native operators continue to exchange Arrow batches. No additional row/Arrow operators are inserted.
The relational node supplies the session zone and cast settings, including in filters and join residuals.

Adjacent temporal calls are fused into one generated evaluator. For example,
`DATE_FORMAT(TO_TIMESTAMP(text), pattern)` parses and formats inside one upcall, with only the
strings crossing the bridge. This preserves Flink's wider intermediate date range and avoids
marshalling an intermediate timestamp. Calendar arithmetic, fractions, NULLs, invalid patterns,
and DST rules follow the running Flink/JDK implementation.

The existing `DATE_FORMAT` and `EXTRACT` `allowIncompatible` options still select their limited
Rust LTZ fast paths. Dynamic patterns and newly supported fields use Flink even with those options.

TIME columns use Arrow millisecond storage even for a declared `TIME(0)`, because Flink's internal
TIME values can retain milliseconds. This prevents precision loss during expression evaluation.

## Timestamp range and opt-in

The engine's existing timestamp column representation is a signed 64-bit count of nanoseconds:
approximately September 1677 through April 2262. Flink timestamps have a wider year range.
Changing only a scalar result to milliseconds would violate the unit assumptions in downstream
joins, unions, keyed state, windows and connectors.

New expressions that must export a timestamp column therefore **fall back by default**. To admit
them for workloads whose timestamp inputs, intermediate Arrow columns and results fit the native
range, set this planner option through Flink configuration:

```sql
SET 'streamfusion.expression.TIMESTAMP_RANGE.allowIncompatible' = 'true';
```

The planning JVM property `-Dstreamfusion.expression.TIMESTAMP_RANGE.allowIncompatible=true`
is also accepted as a compatibility fallback.

The blanket `streamfusion.expression.allowIncompatible` option also enables this path. This is a
range restriction, not approximate arithmetic: representable results use Flink's values and retain
nanoseconds. Generated timestamp results and rounding check overflow instead of wrapping. Outside
the native range, the opt-in path can fail where Flink succeeds. Projected out-of-range timestamp
literals decline at planning time. Existing row/Arrow timestamp input limits remain unchanged.

A fused expression returning STRING, DATE, TIME, an interval, a number or a boolean does not need
the opt-in solely because it has timestamp intermediates. Other operator gates still apply.
Full-range timestamp columns are outside this change. Pattern matching is tracked in
[issue #78](https://github.com/datafusion-contrib/StreamFusion/issues/78).

## Validation and performance

Parity tests cover dynamic patterns and zones, NULLs and errors, pre-epoch fractions, leap days,
month ends, DST gaps/overlaps, temporal casts and intervals, filters, join predicates, grouping,
computed rowtime windows, and watermark timing. Expanded-year tests cover fused string/numeric
results and default fallback for unbounded timestamp outputs. Clock tests check execution-time
bounds rather than equality between two runs.

These changes expand the queries that can remain in an accelerated pipeline. They do not establish
that isolated temporal projections are faster than stock Flink. Release-mode measurements, including
both row/Arrow transposes, are on the [scalar benchmark page](../benchmarks/scalar-functions.md).
