package tech.streamfusion;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

/** Interleaved single-expression measurements, including both row/Arrow boundaries. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class DynamicCollectionBenchmark {
  private static final long ROWS = Long.getLong("collection.rows", 2_000_000L);
  private static final int WARMUP = Integer.getInteger("collection.warmup", 2);
  private static final int RUNS = Integer.getInteger("collection.runs", 5);

  @Test
  void compareExpressions() throws Exception {
    for (boolean map : new boolean[] {false, true}) {
      String name = map ? "MAP" : "ARRAY";
      String expression = map ? "m[lookup_key]" : "arr[idx]";
      TableEnvironment check = environment(map);
      String sql = prepare(check, expression);
      String plan = NativePlanner.explain(check, sql);
      if (!plan.contains("NativeCalc")
          || !plan.contains("RowDataToArrow")
          || !plan.contains("ArrowToRowData")) {
        throw new IllegalStateException("Both transposes must be measured: " + plan);
      }
      double[][] seconds = new double[2][RUNS];
      for (int trial = 0; trial < WARMUP + RUNS; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) % 2;
          double elapsed = run(map, expression, engine == 1);
          if (trial >= WARMUP) {
            seconds[engine][trial - WARMUP] = elapsed;
          }
        }
      }
      System.out.printf(
          Locale.ROOT,
          "[collection] %s rows=%d Flink=%.6fs Native=%.6fs flink_trials=%s"
              + " native_trials=%s%n",
          name,
          ROWS,
          median(seconds[0]),
          median(seconds[1]),
          Arrays.toString(seconds[0]),
          Arrays.toString(seconds[1]));
    }
  }

  private static String prepare(TableEnvironment table, String expression) {
    String select = "SELECT " + expression + " AS v, TRUE AS anchor FROM inputs";
    String type =
        table
            .sqlQuery(select)
            .getResolvedSchema()
            .getColumnDataTypes()
            .get(0)
            .getLogicalType()
            .asSerializableString();
    table.executeSql(
        "CREATE TABLE sink (v " + type + ", anchor BOOLEAN) WITH ('connector' = 'blackhole')");
    return "INSERT INTO sink " + select;
  }

  private static double run(boolean map, String expression, boolean nativeRun) throws Exception {
    TableEnvironment table = environment(map);
    String sql = prepare(table, expression);
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(table) : null;
    long start = System.nanoTime();
    table.executeSql(sql).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (nativeRun && scan.substitutions() == 0) {
      throw new IllegalStateException("Unexpected fallback: " + scan.fallbackReasons());
    }
    return seconds;
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    int middle = sorted.length / 2;
    return sorted.length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
  }

  private static TableEnvironment environment(boolean map) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    if (map) {
      table.createTemporaryView(
          "inputs",
          env.fromSequence(0, ROWS - 1)
              .map(
                  i ->
                      Row.of(
                          i % 8 == 0 ? null : java.util.Map.of("a", i, "b", -i, "c", 7L),
                          i % 7 == 0
                              ? null
                              : new String[] {"a", "b", "c", "missing"}[(int) (i % 4)]))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"m", "lookup_key"},
                      Types.MAP(Types.STRING, Types.LONG),
                      Types.STRING)));
    } else {
      table.createTemporaryView(
          "inputs",
          env.fromSequence(0, ROWS - 1)
              .map(
                  i ->
                      Row.of(
                          i % 8 == 0 ? null : new Long[] {i, null, -i},
                          i % 7 == 0 ? null : (int) (i % 6 - 1)))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"arr", "idx"}, Types.OBJECT_ARRAY(Types.LONG), Types.INT)));
    }
    return table;
  }
}
