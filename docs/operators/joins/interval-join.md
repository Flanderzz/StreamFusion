# Interval join

**Status:** Native. A time-bounded join — a `BETWEEN` predicate on rowtime or proctime instead of
(or alongside) an equi-key match window. Unlike the [regular join](regular-join.md), both inputs
must be insert-only; it is not one of the changelog-aware operators, so a retracting/updating input
falls it back per the [insert-only guard](../index.md#global-switches).

Both **event-time and proctime** bounds are native. An event-time interval join times rows by
rowtime and evicts on the watermark; a proctime interval join times rows by the processing clock and
evicts on a processing-time timer instead.

Interval membership uses **epoch milliseconds**, matching Flink's interval-join
runtime. It does not compare sub-millisecond fractions or scale the interval
into nanoseconds. A runtime TIMESTAMP(3) can still carry such a fraction: with
left time `2.000999999s`, right time `1.000000001s`, and inclusive bounds
`[-1s, +1s]`, Flink matches the pair. Native execution now does too. The timestamp
payload retains its original components; only the interval lookup uses milliseconds.
The direction of the lookup follows the arriving input, including Java long
arithmetic at overflowing bounds.

The native lookup accepts the default millisecond/fraction pair, the four primitive Arrow
timestamp units, and BIGINT milliseconds. Memory snapshots retain
the original payload schema and outer-join match flags; INNER restore does not
interpret a payload column as an outer-join row id. Timestamp payloads and keys preserve both
components across checkpoints, including wide dates and sub-millisecond fractions.

## Admission

Same equi-key/type/residual conditions as the [regular join](regular-join.md): a supported-type
equi-key, null-dropping keys for a non-INNER join, and a non-equi residual the native expression
engine can express. All four join types — INNER, LEFT, RIGHT, and FULL — are native.

## Falls back to Flink when

- the join type isn't INNER, LEFT, RIGHT, or FULL;
- there's no equi key;
- the key columns aren't null-dropping for a non-INNER join;
- the equi-key type is outside the supported set;
- the non-equi residual (the interval bound plus any extra condition) isn't expressible by the
  native expression engine.
