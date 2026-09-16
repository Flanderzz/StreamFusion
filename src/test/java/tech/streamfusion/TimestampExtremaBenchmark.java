package tech.streamfusion;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class TimestampExtremaBenchmark {
  private static final long ROWS = Long.getLong("extrema.rows", 2_000_000L);
  private static final int WARMUP = Integer.getInteger("extrema.warmup", 2);
  private static final int RUNS = Integer.getInteger("extrema.runs", 5);
  private static final String SQL =
      "INSERT INTO sink SELECT k, MIN(ts), MAX(ts) FROM inputs GROUP BY k";

  @Test
  void groupedExtrema() throws Exception {
    for (boolean twoPhase : new boolean[] {false, true}) {
      for (boolean ltz : new boolean[] {false, true}) {
        String plan = NativePlanner.explain(environment(twoPhase, ltz), SQL);
        if (!plan.contains("NativeColumnarGroupAggregate")
            || !plan.contains("RowDataToArrow")
            || !plan.contains("ArrowToRowData")
            || twoPhase && !plan.contains("NativeColumnarLocalGroupAggregate")) {
          throw new IllegalStateException("Expected native aggregate and both transposes: " + plan);
        }
        double[][] times = new double[2][RUNS];
        for (int trial = 0; trial < WARMUP + RUNS; trial++) {
          for (int turn = 0; turn < 2; turn++) {
            int engine = (trial + turn) % 2;
            TableEnvironment table = environment(twoPhase, ltz);
            PhysicalPlanScan scan = engine == 1 ? NativePlanner.install(table) : null;
            long start = System.nanoTime();
            table.executeSql(SQL).await();
            double seconds = (System.nanoTime() - start) / 1e9;
            if (scan != null && scan.substitutions() == 0) {
              throw new IllegalStateException("Unexpected fallback: " + scan.fallbackReasons());
            }
            if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
          }
        }
        double host = median(times[0]);
        double nativeTime = median(times[1]);
        System.out.printf(
            Locale.ROOT,
            "[timestamp-extrema] phase=%s ltz=%s rows=%d Flink=%.6fs Native=%.6fs ratio=%.3fx "
                + "flink_trials=%s native_trials=%s%n",
            twoPhase ? "two" : "one",
            ltz,
            ROWS,
            host,
            nativeTime,
            host / nativeTime,
            Arrays.toString(times[0]),
            Arrays.toString(times[1]));
      }
    }
  }

  private static double median(double[] times) {
    double[] sorted = times.clone();
    Arrays.sort(sorted);
    int middle = sorted.length / 2;
    return sorted.length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
  }

  private static TableEnvironment environment(boolean twoPhase, boolean ltz) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.local-time-zone", "Asia/Shanghai");
    table
        .getConfig()
        .set("table.optimizer.agg-phase-strategy", twoPhase ? "TWO_PHASE" : "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(twoPhase));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.mini-batch.size", "1024");
    LocalDateTime base = LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999000001);
    var dataType = ltz ? DataTypes.TIMESTAMP_LTZ(9) : DataTypes.TIMESTAMP(9);
    table.createTemporaryView(
        "inputs",
        env.fromSequence(0, ROWS - 1)
            .map(
                i -> {
                  LocalDateTime value = base.plusNanos(i % 4096 * 1_000_001);
                  return Row.of(
                      (int) (i % 64),
                      i / 64 % 8 == 0 ? null : ltz ? value.toInstant(ZoneOffset.UTC) : value);
                })
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "ts"},
                    Types.INT,
                    ltz ? Types.INSTANT : Types.LOCAL_DATE_TIME)),
        Schema.newBuilder().column("k", DataTypes.INT()).column("ts", dataType).build());
    String type = dataType.getLogicalType().asSerializableString();
    table.executeSql(
        "CREATE TABLE sink (k INT, mn "
            + type
            + ", mx "
            + type
            + ") WITH ('connector' = 'blackhole')");
    return table;
  }
}
