# Released Flink APIs selected at build time

**Kind:** compatibility boundary — one native engine with separately identified host adapters.

Arroyo supplies the columnar operator model and Comet supplies the Arrow/JNI ownership model.
Neither has Flink's planner ABI, keyed-backend factory signatures or canonical savepoint protocol.
StreamFusion therefore selects a small Java compatibility source root for each released Flink
line. Shared operators continue to consume Arrow batches and keep the same native state encoding;
the adapter changes how the host constructs, wires and snapshots those operators.

Flink 1.18's public RocksDB API cannot independently set the current serialized key and its key
group. Our canonical state keys identify native partitions, so their natural Java hashes need not
belong to the explicitly assigned partition. Reflecting into RocksDB's private serialized-key
builder would couple recovery to implementation details. The native RocksDB wrapper instead uses
a temporary Flink heap backend for its canonical projection, retaining the released snapshot
protocol and clearing live projection state after the snapshot owns its copy. Ordinary JVM
operators keep their RocksDB delegate. The cost is temporary JVM heap proportional to serialized
canonical state. A stock RocksDB delegate is not an admitted native canonical-state carrier on
1.18; the native memory and StreamFusion RocksDB backends are the supported choices. The 1.18
changelog wrapper also recomputes groups from serialized keys during log replay. Native keyed
planning therefore declines changelog-enabled environments instead of unwrapping their backend
and bypassing the durability log. Upstream randomization remains enabled and its actual selected
configuration determines the expected native or fallback contract.

Host semantics that differ remain explicit policy inputs: eager rowtime keep-first, unchanged
mini-batch output with TTL, Jackson decimal normalization, and Parquet map-key nullability. They do
not change the Arrow operator representation or insert rowwise JNI calls. SQL/JSON expressions on
1.18 use the existing host evaluator once per Arrow batch because the older Jackson recycler
contract has not been verified for the native parser emulation.

Each upstream test line owns its checkout, Maven repository, build, classpath and execution
witnesses. This keeps a passing default-line run from accidentally validating the other line's
artifacts. See [Flink compatibility](../docs/flink-compatibility.md) for current admission and the
remaining release gates.
