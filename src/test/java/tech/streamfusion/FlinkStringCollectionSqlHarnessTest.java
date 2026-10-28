package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkStringCollectionSqlHarnessTest {
  private static final RecoveryExecutionMetrics METRICS = new RecoveryExecutionMetrics();

  @AfterAll
  static void closeCluster() {
    METRICS.close();
  }

  private static Schema schema() {
    return Schema.newBuilder().column("k", DataTypes.INT()).column("v", DataTypes.STRING()).build();
  }

  private static List<Row> rows() {
    String[] values = {
      null,
      "",
      "a",
      "é🙂",
      "\ud800",
      "\udc00",
      "?",
      "\ue000",
      "\ud800\udc00",
      "x".repeat(4097),
      "a\u0000b"
    };
    RowKind[] kinds = {
      RowKind.INSERT,
      RowKind.INSERT,
      RowKind.UPDATE_BEFORE,
      RowKind.UPDATE_AFTER,
      RowKind.DELETE,
      RowKind.DELETE,
      RowKind.INSERT
    };
    var rows = new ArrayList<Row>();
    for (int cycle = 0; cycle < 2; cycle++)
      for (int phase = 0; phase < kinds.length; phase++)
        for (int key = 0; key < 3; key++)
          for (String value : values)
            rows.add(
                Row.ofKind(
                    kinds[phase],
                    key == 0 ? null : key,
                    phase == 1 || phase == 5
                        ? null
                        : (phase == 3 || phase == 4) && value != null ? "next:" + value : value));
    return rows;
  }

  private static TableEnvironment environment(String type, int parallelism) {
    return environment(type, parallelism, false);
  }

  private static TableEnvironment environment(String type, int parallelism, boolean notNull) {
    var env = METRICS.environment(new Configuration(), parallelism, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    table.createTemporaryView(
        "base",
        table.fromChangelogStream(
            tech.streamfusion.compat.FlinkTestSources.fromData(
                env, rows(), Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.STRING)),
            notNull
                ? Schema.newBuilder()
                    .column("k", DataTypes.INT())
                    .column("v", DataTypes.STRING().notNull())
                    .build()
                : schema()));
    table.createTemporaryView("src", table.sqlQuery("SELECT k," + value(type) + " AS v FROM base"));
    return table;
  }

  private static String value(String type) {
    return type.equals("STRING") ? "v" : "CAST(v AS " + type + ")";
  }

  private static String operator(String function) {
    return function.equals("COLLECT") ? "NativeJvmGroup" : "NativeColumnarGroupAggregate";
  }

  private static String grouped(String function, boolean global) {
    return "SELECT "
        + (global ? "" : "k,")
        + function
        + "(v),"
        + function
        + "(DISTINCT v),"
        + function
        + "(v) FILTER (WHERE k>0),COUNT(*) FROM src"
        + (global ? "" : " GROUP BY k");
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,STRING,1",
    "COLLECT,STRING,3",
    "COLLECT,CHAR(8),1",
    "COLLECT,CHAR(8),3",
    "ARRAY_AGG,STRING,1",
    "ARRAY_AGG,STRING,3",
    "ARRAY_AGG,CHAR(8),1",
    "ARRAY_AGG,CHAR(8),3"
  })
  void stringFamiliesKeepReleasedChangesAndDeclaredTypes(
      String function, String type, int parallelism) {
    check(function, () -> environment(type, parallelism), grouped(function, false), true);
  }

  @ParameterizedTest
  @CsvSource({"COLLECT,STRING", "COLLECT,CHAR(8)", "ARRAY_AGG,STRING", "ARRAY_AGG,CHAR(8)"})
  void globalCollectionsKeepTheSameInputBoundary(String function, String type) {
    check(function, () -> environment(type, 1), grouped(function, true), false);
  }

  @ParameterizedTest
  @CsvSource({"COLLECT,STRING", "COLLECT,CHAR(8)", "ARRAY_AGG,STRING", "ARRAY_AGG,CHAR(8)"})
  void formattingAndScalarConsumersObserveOwnedResults(String function, String type) {
    check(
        function,
        () -> environment(type, 1),
        "SELECT k,CAST(" + function + "(v) AS STRING) FROM src GROUP BY k",
        true);
  }

  @ParameterizedTest
  @CsvSource({"STRING", "CHAR(8)"})
  void arrayNullPoliciesAndNonNullResultsKeepTheirDeclaredElementType(String type) {
    check(
        "ARRAY_AGG",
        () -> environment(type, 1),
        "SELECT k,ARRAY_AGG(v) RESPECT NULLS,ARRAY_AGG(v) IGNORE NULLS,"
            + "ARRAY_AGG(COALESCE(v,'fallback')) FROM src GROUP BY k",
        true);
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,STRING,hashmap",
    "COLLECT,CHAR(8),hashmap",
    "COLLECT,STRING,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "COLLECT,CHAR(8),tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,STRING,hashmap",
    "ARRAY_AGG,CHAR(8),hashmap",
    "ARRAY_AGG,STRING,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,CHAR(8),tech.streamfusion.state.RocksDBNativeStateBackendFactory"
  })
  void stringStateRecoversAcrossTwoNonemptyPrefixes(String function, String type, String backend)
      throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows(),
                Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.STRING),
                schema(),
                true,
                33,
                132)
            .withExecutionMetrics()) {
      String query =
          "SELECT k," + function + "(v)," + function + "(DISTINCT v),COUNT(*) FROM src GROUP BY k";
      var runs =
          NativeFailureParity.runRecovery(
              () -> recoveryInput(recovery.uninterrupted(), type),
              () -> recoveryInput(recovery.get(), type),
              query);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertEquals(perKey(runs.host().rows()), perKey(runs.nativeRun().rows()));
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), operator(function));
      SharedFlinkCluster.assertNativeMemoryReleased("string-collection-recovery");
    }
  }

  @ParameterizedTest
  @CsvSource({"COLLECT,STRING", "COLLECT,CHAR(8)", "ARRAY_AGG,STRING", "ARRAY_AGG,CHAR(8)"})
  void emptyInputsKeepCollectionTypesWithoutRequiringNonemptyAggregateWork(
      String function, String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    for (boolean keyed : new boolean[] {false, true}) {
      var runs =
          NativeFailureParity.run(
              () -> environment(type, 1),
              "SELECT "
                  + (keyed ? "k," : "")
                  + function
                  + "(v),COUNT(*) FROM src WHERE k<0"
                  + (keyed ? " GROUP BY k" : ""));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertEquals(runs.host().rows(), runs.nativeRun().rows());
    }
  }

  @ParameterizedTest
  @CsvSource({"STRING", "CHAR(8)"})
  void explicitLegacyArrayStateKeepsTheJvmHandler(String type) {
    String property = "streamfusion.experimental.rustCollectionAggregates";
    String previous = System.getProperty(property);
    try {
      System.setProperty(property, "false");
      check(
          "ARRAY_AGG",
          () -> environment(type, 1),
          grouped("ARRAY_AGG", false),
          true,
          "NativeJvmGroup");
    } finally {
      if (previous == null) System.clearProperty(property);
      else System.setProperty(property, previous);
    }
  }

  @ParameterizedTest
  @CsvSource({"COLLECT,STRING", "COLLECT,CHAR(8)", "ARRAY_AGG,STRING", "ARRAY_AGG,CHAR(8)"})
  void materializedNonNullSourceHintsRetainTheHostBoundary(String function, String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var runs =
        NativeFailureParity.run(
            () -> environment(type, 1, true),
            "SELECT CAST(k AS STRING),"
                + function
                + "(v),"
                + function
                + "(COALESCE(k,0)),COUNT(*) FROM src GROUP BY k");
    if (runs.host().failure() == null) runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
    else
      runs.assertFailure(
          runs.host().rootCause().getClass(),
          runs.host().phase(),
          NativeFailureParity.Route.FALLBACK);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertTrue(
        runs.nativeRun().fallbackReasons().stream()
            .anyMatch(reason -> reason.contains("collection input materialization")),
        runs.toString());
  }

  @ParameterizedTest
  @CsvSource({"STRING", "CHAR(8)"})
  void mixedCharacterCollectionsShareTheVerifiedJvmHandler(String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    check(
        "COLLECT",
        () -> environment(type, 1),
        "SELECT k,COLLECT(v),ARRAY_AGG(v),ARRAY_AGG(DISTINCT v),COUNT(*) FROM src GROUP BY k",
        true);
  }

  @ParameterizedTest
  @CsvSource({"STRING", "CHAR(8)"})
  void typedCharacterLiteralsKeepPaddingInProjectionsAndSearchFilters(String type) {
    check(
        "COLLECT",
        () -> environment(type, 1),
        "SELECT k,v,CAST('' AS CHAR(8)),CAST('a' AS CHAR(8)),v=CAST('' AS CHAR(8)),v=CAST('a' AS"
            + " CHAR(8)) FROM src",
        true,
        "NativeCalc");
    check(
        "COLLECT",
        () -> environment(type, 1),
        "SELECT k,v FROM src WHERE v IN (CAST('' AS CHAR(8)),CAST('a' AS CHAR(8)))",
        true,
        type.equals("STRING") ? "NativeFilter" : "NativeCalc");
    check(
        "COLLECT",
        () -> environment(type, 1),
        "SELECT k,v,v IN (CAST('' AS CHAR(8)),CAST('a' AS CHAR(8))),v NOT IN (CAST('' AS"
            + " CHAR(8)),CAST('a' AS CHAR(8))) FROM src",
        true,
        "NativeCalc");
  }

  private static TableEnvironment recoveryInput(TableEnvironment table, String type) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    table.createTemporaryView(
        "src", table.sqlQuery("SELECT k," + value(type) + " AS v FROM recovery_input"));
    return table;
  }

  private static void check(
      String function,
      java.util.function.Supplier<TableEnvironment> factory,
      String sql,
      boolean keyed) {
    check(function, factory, sql, keyed, operator(function));
  }

  private static void check(
      String function,
      java.util.function.Supplier<TableEnvironment> factory,
      String sql,
      boolean keyed,
      String expectedOperator) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var previous =
        METRICS.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs = NativeFailureParity.run(factory, sql);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    if (keyed) assertEquals(perKey(runs.host().rows()), perKey(runs.nativeRun().rows()));
    else assertEquals(runs.host().rows(), runs.nativeRun().rows());
    RecoveryExecutionMetrics.assertConsumed(
        METRICS.nativeOperators().stream()
            .filter(row -> !previous.contains(row.get("jobId")))
            .collect(java.util.stream.Collectors.toList()),
        expectedOperator);
  }

  private static java.util.Map<Object, List<List<Object>>> perKey(List<List<Object>> rows) {
    var result = new java.util.HashMap<Object, List<List<Object>>>();
    for (var row : rows) result.computeIfAbsent(row.get(1), ignored -> new ArrayList<>()).add(row);
    return result;
  }
}
