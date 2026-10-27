package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkCollectionLegacyConsumerSqlHarnessTest {
  private static final RecoveryExecutionMetrics METRICS = new RecoveryExecutionMetrics();

  @AfterAll
  static void closeCluster() {
    METRICS.close();
  }

  private static StreamTableEnvironment environment(int parallelism) {
    var env = METRICS.environment(new Configuration(), parallelism, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    var input =
        tech.streamfusion.compat.FlinkTestSources.fromData(
            env,
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.STRING, Types.LONG),
            Row.of("", -128L),
            Row.of("", -128L),
            Row.of("", null),
            Row.of(null, 32767L),
            Row.of("é🙂", null),
            Row.of("?", Long.MIN_VALUE),
            Row.of("\ud800", Long.MAX_VALUE),
            Row.ofKind(RowKind.UPDATE_BEFORE, "", -128L),
            Row.ofKind(RowKind.UPDATE_AFTER, "", 127L),
            Row.ofKind(RowKind.DELETE, "", -128L),
            Row.ofKind(RowKind.DELETE, "é🙂", null),
            Row.of("é🙂", 7L));
    table.createTemporaryView(
        "src",
        table.fromChangelogStream(
            input,
            Schema.newBuilder()
                .column("k", DataTypes.STRING())
                .column("v", DataTypes.BIGINT())
                .build()));
    return table;
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,TINYINT",
    "COLLECT,SMALLINT",
    "COLLECT,INT",
    "COLLECT,BIGINT",
    "COLLECT,BOOLEAN",
    "ARRAY_AGG,TINYINT",
    "ARRAY_AGG,SMALLINT",
    "ARRAY_AGG,INT",
    "ARRAY_AGG,BIGINT",
    "ARRAY_AGG,BOOLEAN"
  })
  void legacyExternalRowsKeepOrderedChangesAndValues(String function, String type)
      throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    for (int parallelism : new int[] {1, 3}) {
      String value = type.equals("BOOLEAN") ? "(v>0)" : "CAST(v AS " + type + ")";
      String sql =
          "SELECT k,"
              + function
              + "("
              + value
              + "),"
              + function
              + "(DISTINCT "
              + value
              + "),"
              + function
              + "(COALESCE("
              + value
              + ",CAST("
              + (type.equals("BOOLEAN") ? "FALSE" : "0")
              + " AS "
              + type
              + "))),COUNT(*) FROM src GROUP BY k";
      assertEquals(
          perKey(collect(sql, parallelism, false, false)),
          perKey(collect(sql, parallelism, true, true)));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"COLLECT", "ARRAY_AGG"})
  void emptyGlobalLegacyResultsKeepTheirReleasedSemantics(String function) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    String sql = "SELECT " + function + "(v) FROM src WHERE v=42";
    assertEquals(collect(sql, 1, false, false), collect(sql, 1, true, true, "NativeCalc"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"INT", "BIGINT"})
  void ignoreNullsKeepsNullArrayContainersInExternalRows(String type) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    String sql =
        "SELECT k,ARRAY_AGG(CAST(v AS " + type + ")) IGNORE NULLS,COUNT(*) FROM src GROUP BY k";
    assertEquals(perKey(collect(sql, 1, false, false)), perKey(collect(sql, 1, true, true)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"COLLECT", "ARRAY_AGG"})
  void disabledRuntimeRetainsLegacyFallback(String function) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    String property = "streamfusion.experimental.jvmGroupAggregates";
    String previous = System.getProperty(property);
    try {
      System.setProperty(property, "false");
      String sql = "SELECT k," + function + "(v) FROM src GROUP BY k";
      assertEquals(perKey(collect(sql, 1, false, false)), perKey(collect(sql, 1, true, false)));
    } finally {
      if (previous == null) System.clearProperty(property);
      else System.setProperty(property, previous);
    }
  }

  private static List<List<Object>> collect(
      String sql, int parallelism, boolean nativeEnabled, boolean expectNative, String... consumed)
      throws Exception {
    var previous =
        METRICS.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var table = environment(parallelism);
    var scan = nativeEnabled ? NativePlanner.install(table) : null;
    var result = new ArrayList<List<Object>>();
    var query = table.sqlQuery(sql);
    try (var rows = table.toRetractStream(query, Row.class).executeAndCollect()) {
      while (rows.hasNext()) {
        var next = rows.next();
        var fields = new ArrayList<Object>();
        fields.add(next.f0);
        for (int i = 0; i < next.f1.getArity(); i++)
          fields.add(NativeParity.comparableValue(next.f1.getField(i)));
        result.add(fields);
      }
    }
    var work =
        METRICS.nativeOperators().stream()
            .filter(row -> !previous.contains(row.get("jobId")))
            .collect(java.util.stream.Collectors.toList());
    if (expectNative) {
      assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
      if (consumed.length == 0)
        RecoveryExecutionMetrics.assertConsumed(work, "NativeColumnarGroupAggregate");
      else for (String operator : consumed) RecoveryExecutionMetrics.assertConsumed(work, operator);
    } else assertTrue(work.isEmpty(), work.toString());
    SharedFlinkCluster.assertNativeMemoryReleased("collection-legacy-consumer");
    return result;
  }

  private static java.util.Map<Object, List<List<Object>>> perKey(List<List<Object>> rows) {
    var result = new java.util.HashMap<Object, List<List<Object>>>();
    for (var row : rows) result.computeIfAbsent(row.get(1), ignored -> new ArrayList<>()).add(row);
    return result;
  }
}
