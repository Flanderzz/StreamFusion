package tech.streamfusion;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/** Fail only after a checkpoint containing a nonempty aggregate prefix has completed. */
final class PortableSqlRecovery implements Supplier<TableEnvironment>, AutoCloseable {
  private static final Map<String, Proof> PROOFS = new ConcurrentHashMap<>();
  private final List<String> runIds = new java.util.ArrayList<>();

  static final class Proof {
    final AtomicBoolean failed = new AtomicBoolean();
    final AtomicInteger restoredOffset = new AtomicInteger(-1);
    final AtomicInteger completions = new AtomicInteger();
  }

  @Override
  public TableEnvironment get() {
    String runId = UUID.randomUUID().toString();
    runIds.add(runId);
    PROOFS.put(runId, new Proof());
    Configuration config = new Configuration();
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(10));
    var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(1);
    env.enableCheckpointing(50);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.createTemporaryView(
        "recovery_input",
        env.addSource(new RecoveringSource(runId))
            .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG)),
        Schema.newBuilder().column("k", DataTypes.INT()).column("v", DataTypes.BIGINT()).build());
    return table;
  }

  void verify() {
    org.junit.jupiter.api.Assertions.assertEquals(2, runIds.size(), "both engines must execute");
    for (String runId : runIds) {
      Proof proof = PROOFS.get(runId);
      org.junit.jupiter.api.Assertions.assertTrue(
          proof.failed.get(), "failure injection never fired");
      org.junit.jupiter.api.Assertions.assertEquals(
          32, proof.restoredOffset.get(), "did not restore the nonempty source checkpoint");
      org.junit.jupiter.api.Assertions.assertTrue(
          proof.completions.get() > 0, "checkpoint never completed");
    }
  }

  List<Map<String, Object>> observations() {
    return runIds.stream()
        .map(
            id -> {
              Proof proof = PROOFS.get(id);
              return Map.<String, Object>of(
                  "failedAfterCheckpoint",
                  proof.failed.get(),
                  "restoredSourceOffset",
                  proof.restoredOffset.get(),
                  "completedCheckpoints",
                  proof.completions.get());
            })
        .toList();
  }

  @Override
  public void close() {
    runIds.forEach(PROOFS::remove);
  }

  private static final class RecoveringSource
      implements SourceFunction<Row>, CheckpointedFunction, CheckpointListener {
    private final String runId;
    private volatile boolean running = true;
    private volatile boolean completedPrefix;
    private transient ListState<Integer> state;
    private transient Map<Long, Integer> snapshots;
    private int next;
    private boolean restored;

    private RecoveringSource(String runId) {
      this.runId = runId;
    }

    @Override
    public void run(SourceContext<Row> context) throws Exception {
      if (!restored) {
        emitThrough(context, 32);
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (running && !completedPrefix && System.nanoTime() < deadline) Thread.sleep(5);
        if (!running) return;
        if (!completedPrefix)
          throw new IllegalStateException("no checkpoint captured the source prefix");
        if (PROOFS.get(runId).failed.compareAndSet(false, true)) {
          throw new IllegalStateException("portable source failure after completed checkpoint");
        }
      }
      emitThrough(context, 96);
    }

    private void emitThrough(SourceContext<Row> context, int end) {
      synchronized (context.getCheckpointLock()) {
        while (running && next < end) {
          context.collect(Row.of(next % 3, (long) next));
          next++;
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      state.update(List.of(next));
      snapshots.put(context.getCheckpointId(), next);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      state =
          context
              .getOperatorStateStore()
              .getListState(new ListStateDescriptor<>("source-offset", Integer.class));
      snapshots = new ConcurrentHashMap<>();
      restored = context.isRestored();
      if (restored) {
        for (int offset : state.get()) next = offset;
        PROOFS.get(runId).restoredOffset.set(next);
      }
    }

    @Override
    public void notifyCheckpointComplete(long id) {
      if (snapshots.getOrDefault(id, 0) == 32) {
        PROOFS.get(runId).completions.incrementAndGet();
        completedPrefix = true;
      }
      snapshots.keySet().removeIf(checkpoint -> checkpoint <= id);
    }
  }
}
