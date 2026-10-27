# Rust collection aggregate state

Verified integer/BOOLEAN COLLECT and Flink 2.2 ARRAY_AGG execute their accumulation,
retraction and result materialization inside the existing Rust keyed aggregate. Arrow batches
cross JNI once per push, avoiding the preceding generated JVM handler's rowwise aggregate work.
The source/sink transposes remain at the native island's perimeter.

COLLECT stores signed 32-bit multiplicities and models Java map iteration, including the fresh
map copy used by the released result handler. ARRAY_AGG stores arrival order and removes the
first matching retraction. The common DISTINCT retention machinery preserves shared-filter
write policies independently from collection contents. Result Arrow buffers own their contents;
later input cannot mutate an already emitted changelog row.

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
