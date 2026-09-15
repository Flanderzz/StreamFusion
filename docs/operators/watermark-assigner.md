# Watermark assigner

**Status:** partial, for rowtime minus a constant day-time or year-month interval.

Native admission accepts `WATERMARK FOR rt AS rt` and `rt - INTERVAL constant` when the constant
is a non-negative **day-time** or **year-month** interval. DAY, HOUR, MINUTE, SECOND and composite
forms such as DAY TO SECOND carry milliseconds; YEAR, MONTH and YEAR TO MONTH carry calendar
months. Zero delay is supported.

Calendar subtraction uses Flink's `DateTimeUtils.addMonths`, including month-end clamping and
leap years. Each Arrow row's candidate is calculated **before** taking the running maximum:
`MAX(rt - interval)`. For example, March 30 at 23:00 and March 31 at 00:00 both map to the last
day of February when subtracting one month, but the first candidate is later. Taking the maximum
rowtime first would lose that candidate. This also preserves Flink's signed integer arithmetic
for fixed delays at the range limits. Rowtime columns keep their existing representation.

The independent assigner starts at watermark zero and slices out-of-order batches when an eager
watermark must precede a later row, matching Flink's late-row behavior. A NULL rowtime fails the
job like Flink's assigner. Non-constant or negative delays, other watermark expressions, and
expressions referring to a different column fall back.

The assigner can follow a columnar producer or a rowwise source leaf. The transition pass inserts
the source-edge transpose when needed; the whole query still has to satisfy the
[all-or-nothing island rule](index.md#the-all-or-nothing-island).

Watermarks pushed into a source use the same interval parsing and candidate evaluation. Source
generators start at `Long.MIN_VALUE` and ignore NULL candidates, matching Flink's pushed generator.
The maximum candidate is separate from the batch's event timestamp and remains available after
downstream consumers release its Arrow buffers. Their additional
admission rules are documented for [Kafka](../connectors/kafka.md#source-admission-and-fallbacks)
and [Paimon](../connectors/paimon.md).

## Watermark expressions

`SOURCE_WATERMARK()` uses the source's existing watermark forwarding. `CURRENT_WATERMARK(rt)` in a
native Calc reads that Calc's last received watermark and returns NULL before the first one. It also
works in a pure filter, which uses the Calc runtime when a watermark context is needed. The value is
scoped to the synchronous evaluation, with the previous context restored afterwards, so it cannot leak between operators
sharing a task thread. See [temporal functions](temporal-functions.md) for contextual restrictions.
