# Retracting window buffers

Arroyo's tumbling aggregation uses DataFusion partial/final plans over buffered bins.
Its planner rejects aggregation over updating Debezium input, so it has no equivalent
retracting window implementation to transplant. We retain StreamFusion's existing
columnar window assignment, per-window accumulators and local/global split.

RisingWave's hop-window executor preserves each input change when duplicating rows into
windows, normalizing update-before/after into delete/insert. Its hash aggregator owns
signed group state and watermark cleanup. We use the same separation: assignment does
not discard the change sign; aggregation owns group liveness. Flink's concrete buffers
still determine our representation and results.

Released Flink 2.2.1 uses a nullable sum and signed non-NULL count for retracting SUM,
plus a separate live-row count (or a user COUNT(*)). Zero means empty; a negative count
does not. These fields travel in Flink's local partial order, including across a local
barrier drain, and use the existing raw-keyed and direct RocksDB checkpoint paths.
Integral SUM/COUNT are admitted first; unsupported retracting forms remain explicit
planner fallbacks rather than borrowing append-only accumulator semantics.

FLOAT/DOUBLE SUM extends the same nullable sum/count layout. It follows Flink's declared
sum precision and assigns the first non-NULL value directly to retain negative zero.
Subsequent arithmetic and partial merges stay ordered; a zero-count partial can carry a
nonzero or nonfinite sum that must survive checkpointing. No new window/state architecture
is needed for these types.

DECIMAL SUM also keeps this two-field layout, widening the sum to precision 38 while
preserving the input scale. Its arithmetic reuses the append-only decimal SUM primitive:
overflow produces a NULL sum that the next signed value can reset. The signed count and
zero-count residuals remain independent of that nullable sum, as in Flink's retracting SUM.

The Arrow ownership pattern was checked against Comet's ColumnarBatchArrowReader:
producer vectors must not be closed through a second owning root. The window projection
therefore copies the change-kind byte into a vector owned by its exported root, just as
it owns its projected values and keys. The original input keeps its own lifetime.

References consulted before implementation:

- Arroyo `crates/arroyo-worker/src/arrow/tumbling_aggregating_window.rs` and
  `crates/arroyo-planner/src/test/queries/error_no_aggregate_over_debezium.sql`
  at `de9f70f7203ea9e91fe3e1d8d658e82d4662cb9a`.
- RisingWave `src/stream/src/executor/hop_window.rs` and `aggregate/hash_agg.rs`
  at `216a211fc408a2f195615f922781567b3be6adaa`.
- Comet `spark/src/main/scala/org/apache/spark/sql/comet/execution/arrow/ColumnarBatchArrowReader.scala`
  and `native/jni-bridge/src/arrow_array_stream.rs` at
  `ef62b46306e925bc51e7d7f29922c1870eb729e7`.
