# Rust collection aggregate state

Verified integer/BOOLEAN, DATE/TIME, FLOAT/DOUBLE and DECIMAL COLLECT and Flink 2.2 ARRAY_AGG execute their accumulation,
retraction and result materialization inside the existing Rust keyed aggregate. Arrow batches
cross JNI once per push, avoiding the preceding generated JVM handler's rowwise aggregate work.
The source/sink transposes remain at the native island's perimeter.

COLLECT stores signed 32-bit multiplicities and models Java map iteration, including the fresh
map copy used by the released result handler. ARRAY_AGG stores arrival order and removes the
first matching retraction. The common DISTINCT retention machinery preserves shared-filter
write policies independently from collection contents. Result Arrow buffers own their contents;
later input cannot mutate an already emitted changelog row.

Result emission reuses the immutable Arrow result type already owned by the group operator,
avoiding per-row field/type reconstruction identified in the first CPU profile.

Collection state uses a versioned native snapshot in the existing heap/RocksDB store and
key-group partitioning. `streamfusion.experimental.rustCollectionAggregates=false` retains the
preceding managed JVM handler for its savepoints and for comparisons. Mixed groups whose other
functions require the JVM keep their verified handler; richer collection types and consumers
retain the same admission guards. [Coverage and restore rules](../operators/group-by.md#integer-collect)
describe the exact boundary.

[Collection benchmark history](../benchmarks/collection-aggregates.md) retains stock/previous
controls and complete-job measurement boundaries. Consumed native work proves the Rust route,
not a speedup. Collection snapshots, output materialization and both perimeter conversions
remain costs; performance investigations follow the accepted two-pass optimization timebox.

The [release comparison](../benchmarks/collection-aggregates.md#rust-collection-state) reports
all trials and ranges. For 50,000 rows and 64 groups, BIGINT ARRAY_AGG takes 0.264972 s median
versus stock's 0.664409 s and the preceding JVM route's 0.776970 s. All four measured primitive
ARRAY_AGG shapes improve stock at that cardinality; high-cardinality ranges overlap more.
COLLECT retains slower BOOLEAN/narrow-integer cases. These are workload-specific complete-job
results, not a universal throughput claim. Two CPU captures close the optimization timebox;
generic result materialization and boxed arrays at row exits remain follow-up work in
[#239](https://github.com/datafusion-contrib/StreamFusion/issues/239).


DATE and TIME reuse this state layout with separate logical type tags and the existing Java
boxed-integer hashing/order contract. Arrow date/time columns are downcast once per batch;
no temporal-to-integer row conversion is introduced inside the operator. Their nullable-array
and representation gates are [documented separately](../operators/group-by.md#date-and-time-collection-elements).


## One state algorithm with typed element codecs

All verified scalar element families use the same collection state machine. Codecs supply
compact owned values, typed Arrow conversion and Java boxed hashing/comparison. Floating codecs
reuse the existing map-order key: comparisons canonicalize NaNs and distinguish signed zero,
while retained raw bits survive output and checkpoints. Decimal codecs reuse the BigDecimal
hash model and retain unscaled i128 values plus precision/scale. This adds families without
copying accumulation, retraction, DISTINCT, TTL or Java map-copy logic.

Typed readers are selected once per batch; per-row updates remain native and do not construct
ScalarValue objects. The primitive codec preserves the existing version-one JSON envelope and
field layout byte for byte. Floating snapshots encode comparator/raw bit patterns losslessly;
decimal snapshots carry the exact logical parameters and reject other scales/precisions.
The existing Arrow batch ownership and heap/RocksDB state boundaries remain unchanged.


The [shared-backend comparison](../benchmarks/collection-aggregates.md#shared-typed-collection-state)
retains 480 complete jobs, 150 positive native group witnesses and previous native BIGINT controls.
At 64 groups DOUBLE and compact decimal ARRAY_AGG beat paired stock by 2.92×/1.73×; wide decimal
arrays and the measured numeric COLLECT cases retain losses. Two CPU captures close the bounded
investigation: output emission and Java decimal reconstruction/copying remain costs in #239.
