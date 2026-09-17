# Ordered grouped values

Arroyo's aggregation operators use DataFusion's partial/final plans inside window
operators. They do not supply a matching non-windowed, retractable grouped first/last
operator. StreamFusion keeps the existing Arrow group-aggregate operator and extends
its per-key accumulator state.

RisingWave's `first_last_value.rs` and aggregate `minput.rs` are related references:
append-only aggregation retains a value, while retractable aggregation materializes
ordered inputs and reads an extreme from an ordered cache. Its first/last aggregates
retain NULLs and use explicit ordering columns, which differ from released Flink's
one-argument functions.

We retain one scalar for append-only FIRST_VALUE/LAST_VALUE and an ordered sequence
of non-NULL occurrences for retractable input. Flink's one-argument retractable functions
assign processing-time order and preserve insertion order within equal milliseconds;
the sequence models arrival order, including removal of the oldest matching duplicate.
It does not impose event-time or SQL sort order. Wall-clock regressions are inherently
timing-dependent; portable parity fixtures control arrival order.

Flink's retractable first/last value and order maps have independent entry TTL. A live
group can retain an accumulator value whose supporting map entry has expired. Until both
lifetimes and their lazy reads are represented, positive retention gates these retractable
forms; append-only scalar state and SINGLE_VALUE retain ordinary group TTL.

SINGLE_VALUE uses Flink's separate nullable value and element count. NULL consumes an
element, so eager NULL skipping would incorrectly admit a second value. Cardinality
errors cross JNI as Flink's own exception class after imported Arrow buffers have been
released, retaining the ownership pattern checked against Comet's JNI Arrow import path.

Ordered occurrences checkpoint through the existing Arrow side-batch framing, preserving
per-key occurrence order. These new accumulator kinds use the raw keyed snapshot fallback
on RocksDB rather than treating a value multiset as an ordered table. A direct persistent
representation needs explicit order keys before it can replace that fallback. Local/global
partial schemas remain gated until their merge semantics are implemented.
