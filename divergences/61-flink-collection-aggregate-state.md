# Collection aggregate state follows Flink's changelog contracts

COLLECT and ARRAY_AGG reuse the existing Arrow-batch keyed aggregate operator and its
heap/RocksDB checkpoint boundaries. They do not introduce a second operator or JNI ownership
protocol. Arroyo's updating-aggregate planner delegates aggregate work through DataFusion;
StreamFusion's existing keyed aggregate instead preserves the released Flink state contracts.

DataFusion's ordinary collection aggregate state is not a substitute for those contracts.
Flink COLLECT uses signed 32-bit multiplicities, including negative and zero counts, and a
fresh Java HashMap result snapshot. ARRAY_AGG keeps arrival order, removes the first matching
retracted element, and retains unmatched retractions separately. DISTINCT membership has its
own shared-filter TTL/write policy, separate from the retained aggregate result. Rust state
therefore follows the released CollectAggFunction, ArrayAggFunction and StateMapView behavior,
using the project's Java map-order model and shared DISTINCT retention machinery.

The collection snapshot envelope carries a version, aggregate kind and primitive element type.
JVM-managed collection state and this native format are intentionally separate. Restoring a
JVM savepoint requires `streamfusion.experimental.rustCollectionAggregates=false`; automatic
migration is not claimed. The existing JVM route also remains available for verified mixed
aggregate groups whose other functions do not have a Rust implementation. Richer collection
signatures and consumer boundaries retain their existing admission guards.


DATE and TIME extend the existing counted/sequence state with dedicated primitive tags.
Their internal epoch-day/millisecond values use the same boxed-integer hash/equality contract,
while restore and DISTINCT membership preserve the logical type. NOT NULL temporal arrays
retain fallback until their released generic/binary copy category is verified end to end.

NOT NULL DATE/TIME arguments also retain host execution: actual NULL source slots under those
hints succeed on released Flink but fail Arrow batch import. This admission guard avoids
changing the shared bridge or silently strengthening Flink logical hints into constraints.
