# Batch reuse / multi-consumer sharing in the reference engines (2026-07-28)

Question that prompted this: q3 (and every Nexmark query joining two views of the one `nexmark`
topic) runs TWO native Kafka sources because `NativeRelDigests` barriers Flink sub-plan reuse —
our `ArrowBatch` is consumed destructively (single owner closes the buffers), so a merged native
subtree would double-free. Measured cost: the topic is read+decoded twice; q3's per-event wall
cost is 0.45 s/M vs Flink's 0.28 s/M (Flink reuses its scan). How do the reference engines
handle one batch feeding multiple consumers?

## DataFusion Comet — NO batch sharing (and it's the source of our move semantics)

- FFI crossings are strict single-owner moves: JVM export → native `std::ptr::read` takes
  ownership (`native/core/src/execution/jni_api.rs:1290`); native export is literally
  `move_to_spark` (`native/core/src/execution/utils.rs:30`); the JVM consumer is sole owner and
  frees by `CometVector.close()`.
- Native operators recycle the same buffers for successive batches — `CometExecIterator.hasNext`
  must close the previous batch before polling the next (`CometExecIterator.scala:188-194`). A
  batch cannot even be retained across iterations, let alone shared.
- Plan-level reuse rides Spark's materialization: `ReusedExchangeExec` re-reads shuffle files /
  cached broadcast (`CometExecRule.scala:304,348`); no native subtree ever has two parents;
  DataFusion streams are polled by exactly one parent.
- Why Comet can afford this: Spark's exchanges materialize, so "reuse" never needs live batch
  fan-out. Streaming has no such materialization point — the constraint doesn't transfer to us.

## Arroyo — YES: DAG + named-node dedup + Arc-shared batches (the model to mirror)

- Physical/logical graphs are petgraph `DiGraph`s; a node can have multiple distinct out-edges
  (`arroyo-datastream/src/logical.rs:274`, `arroyo-worker/src/engine.rs:162,714-743`).
- Plan-build dedup via `named_nodes: HashMap<NamedNode, NodeIndex>` where `NamedNode` is
  `Source | Watermark | RemoteTable | Sink` (`arroyo-planner/src/builder.rs:49,227-233,362-367`):
  a second reference to the same source short-circuits (`TreeNodeRecursion::Jump`) and wires an
  edge to the existing node. Two views over one Kafka topic = ONE source, decoded once. Dedup is
  deliberately limited to named nodes — arbitrary identical subplans are not merged.
- Fan-out: `ArrowCollector::collect` repartitions per outgoing edge from the same `RecordBatch`
  via `slice`/column `clone` — Arc refcount bumps, zero copy (`arroyo-operator/src/context.rs:
  506-606`). Keyed edges redo hashing per edge (compute, not copy). Cross-worker edges serialize
  via Arrow IPC as expected.

## RisingWave — YES on all levels, on by default

- Shared sources: `CREATE SOURCE` spawns a dedicated source streaming job; every MV consumes it
  via `StreamSourceScan` → merge + `SourceBackfillExecutor` (forwarded chunks carry split/offset
  columns; backfill reconciles history). Kafka read+decode happens ONCE
  (`session_config/mod.rs:393-399`, `stream_source_scan.rs:37-42`,
  `source_backfill_executor.rs:171-234`). Default-enabled.
- Fragment graph is a true DAG; the dispatcher fans a `StreamChunk` to N channels by
  `chunk.clone()` — columns are `Arc<[ArrayRef]>`, so clone = refcount bump
  (`dispatch.rs:243,807-818`, `data_chunk.rs:65,171`). Hash dispatch shares columns and varies
  only visibility bitmaps. Remote consumers get one serialization per destination.
- Optimizer dedup: `LogicalShare`/`StreamShare` ("the key operator for DAG plan"), a
  common-subplan-sharing pass, and a dedicated `ShareSourceRewriter` that rewrites any source
  referenced >1× to one shared node — run even when general plan-sharing is off, to keep
  self-source-joins correct (`logical_share.rs:31-46`, `merge_eq_nodes.rs:36-70`,
  `share_source_rewriter.rs:40-72`). `StreamShare` lowers to a no-shuffle exchange emitted once
  and referenced by multiple parents (`stream_share.rs:113`). Batch (non-streaming) plans are
  un-shared back to trees (`dag_to_tree_rule.rs`).

## Implications for StreamFusion

- Comet's single-owner FFI is the right pattern per crossing, but its no-sharing stance is an
  artifact of Spark's materialized exchanges; both streaming-native engines converge on:
  **dedupe the source at plan time, fan out refcounted batches at runtime.**
- Arroyo's scope is the pragmatic one to mirror (reference-first rule): dedup only *named*
  prefixes — for us the native source (and its watermark assigner) — not arbitrary subplans.
  That alone fixes q3/q4/q8/q9/q20's doubled topic read.
- Runtime mechanism on our side: Arrow Java already refcounts buffers (`ReferenceManager`
  retain/release). A share/tee point can hand each consumer its own retained view of the
  `VectorSchemaRoot` (per-consumer `retain()` + separate root; each consumer's existing
  `in.close()` then decrements instead of freeing). The reuse barrier stays for everything
  downstream of the tee; only the deduped prefix relaxes single-consumer.
