# Timestamp values without a nanosecond range restriction

Flink 2.2.1 `TimestampData` stores signed milliseconds and a nanosecond remainder
in `0..999999`. Converting that pair to an `i64` nanosecond count loses Flink's
range. Converting negative nanoseconds to milliseconds with truncating division
also changes event time: `-1ns` belongs to millisecond `-1`, not `0`.

The shared Rust bridge and Java Arrow accessor read the components directly from
each primitive Arrow timestamp unit. Millisecond columns retain their full range;
microseconds and nanoseconds use Euclidean division/remainder. Seconds are checked
when expanding to milliseconds. Accessors borrow existing vectors; they do not
retain or release their buffers. This follows Comet's explicit separation between
column views and Arrow buffer ownership. A millisecond-to-Int64 column view shares
the original buffers.

Flink BinaryRow key serialization uses the components with the logical timestamp
precision supplied by the planner. This preserves Flink's compact and non-compact
key bytes, including timestamps nested in arrays, without a nanosecond intermediate.
Temporal sorting, window-aggregate input, source/assigned watermarks and JVM temporal
arguments read milliseconds through the same contract. Function-specific calendar
arithmetic remains separate: Flink's EXTRACT may intentionally divide a timestamp's
milliseconds toward zero when selecting its calendar day.

The SQL column layout and checkpoint encoding have not changed in this step.
Old nanosecond sort snapshots are read through the same accessor and tested across
restore. Full-range key tests construct Arrow columns directly and compare their
bytes with Flink's runtime serializer; they are not evidence of full-range SQL
support. The writer, remaining native consumers, expression outputs, connector
boundaries and persisted row encodings still need coordinated migration before
[#64](https://github.com/datafusion-contrib/StreamFusion/issues/64) can be closed
or wide-range timestamp producers can be admitted.

This is correctness and migration groundwork, not a performance claim. It adds no
SQL functions or opt-in compatibility setting.
