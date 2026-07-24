package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * A/B of the native island's state backends on Nexmark q4: the same native job once on the default
 * (resident memory, raw keyed-state snapshots) backend and once on the Paimon backend (read-through
 * probes against local parquet tables, one incremental commit per barrier), both under the same
 * checkpoint interval — the backend's entire cost lives on the checkpoint path, so a comparison
 * without checkpointing would measure nothing. q4's two GROUP BY aggregates carry the per-key state
 * (~auction-count keys on the inner MAX); its interval join is watermark-driven and stays on memory
 * state under both backends, contributing equal cost to each side.
 *
 * <p>Opt-in like the other end-to-end benchmarks: {@code SF_BENCHMARK=true} under {@code -Pbench}.
 * Before timing anything, a small run is watched for a live {@code Paimon*Store} native handle —
 * the backend's local tables are deleted on task close, so engagement is proven while the job
 * runs, never inferred from configuration.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class PaimonStateBackendBenchmark {

  private static final long ROWS =
      System.getenv("SF_ROWS") != null ? Long.parseLong(System.getenv("SF_ROWS")) : 2_000_000L;
  private static final int WARMUP = 1;
  private static final int RUNS = 2;
  private static final String CHECKPOINT_INTERVAL = "500 ms";

  private static final String SINK_DDL =
      "CREATE TABLE nexmark_q4 (id BIGINT, final BIGINT) WITH ('connector' = 'blackhole')";
  private static final String INSERT_SQL =
      "INSERT INTO nexmark_q4 SELECT Q.category, AVG(Q.final) FROM (SELECT MAX(B.price) AS final,"
          + " A.category FROM auction A, bid B WHERE A.id = B.auction AND B.`dateTime` BETWEEN"
          + " A.`dateTime` AND A.expires GROUP BY A.id, A.category) Q GROUP BY Q.category";

  @Test
  void q4MemoryVersusPaimonBackend() throws Exception {
    assertPaimonEngages();
    double memory = bestOf(backendConfiguration(false));
    double paimon = bestOf(backendConfiguration(true));
    System.out.printf(
        "%n[benchmark] q4 native, %s checkpoints, over %,d events (best of %d)%n",
        CHECKPOINT_INTERVAL, ROWS, RUNS);
    System.out.printf(
        "[benchmark]   memory backend: %6.3f s  (%,.0f events/s)%n", memory, ROWS / memory);
    System.out.printf(
        "[benchmark]   paimon backend: %6.3f s  (%,.0f events/s)  %.2fx vs memory%n",
        paimon, ROWS / paimon, memory / paimon);
  }

  private static Configuration backendConfiguration(boolean paimon) {
    Configuration configuration = new Configuration();
    configuration.setString("execution.checkpointing.interval", CHECKPOINT_INTERVAL);
    if (paimon) {
      configuration.setString(
          "state.backend.type", "io.github.jordepic.streamfusion.state.PaimonStateBackendFactory");
    }
    return configuration;
  }

  private static double bestOf(Configuration configuration) throws Exception {
    double best = Double.MAX_VALUE;
    for (int run = 0; run < WARMUP + RUNS; run++) {
      double seconds = runOnce(configuration, ROWS);
      if (run >= WARMUP) {
        best = Math.min(best, seconds);
      }
    }
    return best;
  }

  private static double runOnce(Configuration configuration, long rows) throws Exception {
    TableEnvironment tEnv = NexmarkBenchmark.environment(rows, configuration);
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    tEnv.executeSql(SINK_DDL);
    long start = System.nanoTime();
    tEnv.executeSql(INSERT_SQL).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (scan.substitutions() == 0) {
      throw new IllegalStateException(
          "native substitution did not engage; comparison is moot. fallback reasons: "
              + scan.fallbackReasons());
    }
    return seconds;
  }

  /**
   * Proves the Paimon backend actually takes over this job's aggregates (a losing gate falls back
   * to memory state silently, which would turn the A/B into memory vs memory): while a small run
   * executes, the live native handle breakdown must show a Paimon-store-backed operator.
   */
  private static void assertPaimonEngages() throws Exception {
    AtomicBoolean seen = new AtomicBoolean();
    Thread watcher =
        new Thread(
            () -> {
              while (!seen.get() && !Thread.currentThread().isInterrupted()) {
                if (Native.liveNativeHandles().contains("Paimon")) {
                  seen.set(true);
                  return;
                }
                try {
                  Thread.sleep(50);
                } catch (InterruptedException e) {
                  return;
                }
              }
            });
    watcher.setDaemon(true);
    watcher.start();
    try {
      runOnce(backendConfiguration(true), 100_000);
    } finally {
      watcher.interrupt();
      watcher.join();
    }
    if (!seen.get()) {
      throw new IllegalStateException(
          "the Paimon backend never engaged for q4 (no live Paimon store handle was observed);"
              + " the A/B would compare memory against memory");
    }
  }
}
