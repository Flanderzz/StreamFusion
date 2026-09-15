# Timestamp components and event-time readers

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
Temporal sorting, window-aggregate input and source/assigned watermarks read milliseconds
through the same contract. Generated JVM temporal arguments preserve both components;
millisecond-only builtins consume the millisecond component. Function-specific calendar
arithmetic remains separate: Flink's EXTRACT may intentionally divide a timestamp's
milliseconds toward zero when selecting its calendar day.

The default SQL timestamp representation is an Arrow struct with `millis: Int64` and
`nano_of_milli: Int32`, both non-null children under the timestamp's parent validity bitmap.
Each child carries `streamfusion.timestamp.component` metadata naming the component; an ordinary
ROW with the same field names is not a timestamp. This keeps Flink's two-part value in separate
columnar buffers, without narrowing it to an i64 nanosecond count. Readers borrow those buffers,
and a millisecond projection shares the millis buffer and parent null bitmap. Writers preserve
fractions present in a runtime value regardless of declared precision. Timestamp literals, generated
JVM results, clocks, rounding, window boundaries and native key codecs use the same representation.

DataFusion orders the struct lexicographically: signed milliseconds first, non-negative fractional
nanos second. That is timestamp order without an overflow-prone conversion. Arrow IPC and row-state
codecs preserve both components. New memory/RocksDB and canonical checkpoint tests cover wide dates,
fractional ordering and recovery. Snapshot versions changed because old Arrow row bytes describe a
different layout. Old snapshots are rejected before decoding; there is no old-state conversion in
this migration. See the [upgrade contract](../docs/backends/canonical-state.md#timestamp-layout-upgrade).

Interval joins also consume milliseconds through this contract. Their interval
filter remains inside the DataFusion hash join, following Arroyo's buffered-batch
join structure. Flink's `TimeIntervalJoin` determines bounds from the arriving
row and probes the opposite cache in milliseconds, even when a TIMESTAMP(3)
payload contains a sub-millisecond remainder. The native predicate mirrors those
integer comparisons and Java long overflow in each arrival direction, without
timestamp-plus-duration intermediates. The payload is not narrowed. Primitive
layouts, both arrival orders, all four join types, and match flags across memory
restore are tested; SQL probes cover both fractional interval endpoints.

The independent windowing TVF also reads the millisecond component, fixing pre-epoch fractions
that truncating nanosecond division assigned to the next window. It fans out payload columns with
Arrow `take`, retaining the original layout/remainder. Boundary calculation stays in milliseconds;
conversion to the caller's physical output type is explicit and checked for overflow. The SQL JNI
entry point requests the component layout. Primitive-boundary tests compose assignment with a
downstream window join and IPC snapshot restore. This keeps Arroyo's columnar batch structure but
uses Flink's signed millisecond clock rather than Arroyo's SystemTime/nanosecond representation.

Connectors convert at their physical format boundary. JSON and CSV preserve the complete timestamp,
including wide dates and hidden fractions. Avro retains Flink's millisecond wire semantics. ORC uses
the released orc-rust Decimal128 nanosecond decoder before splitting into components. Parquet's
released Arrow reader exposes INT96 through i64 units only, so INT96 columns use aligned millisecond
and nanosecond reads. Wrapping subtraction of `millis * 1000000` from the nanosecond read recovers the
fraction exactly while the millisecond read retains the date. This incurs a second column decode
for INT96; other physical timestamps use one. The admission memory estimate includes both readers.
No dependency forks or row transposes are introduced. Parquet sink unit conversion matches Flink's
flooring and Java long overflow at the physical INT64 boundary; selecting nanoseconds still limits
the file's date range to roughly 1677–2262. Internal values remain lossless. Connector-local timezone
metadata labels LTZ without changing the value.

This is a correctness change, with no performance claim. It removes the timestamp-result range
opt-in; independent function and connector admission conditions remain in force.
