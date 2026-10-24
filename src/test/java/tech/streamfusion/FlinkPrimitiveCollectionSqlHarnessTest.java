package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

class FlinkPrimitiveCollectionSqlHarnessTest {
  private static final RecoveryExecutionMetrics METRICS = new RecoveryExecutionMetrics();

  @AfterAll
  static void closeCluster() {
    METRICS.close();
  }

  private static Schema schema(String type) {
    return Schema.newBuilder()
        .column("k", DataTypes.INT())
        .column(
            "v",
            type.equals("BOOLEAN")
                ? DataTypes.BOOLEAN()
                : type.equals("TINYINT") ? DataTypes.TINYINT() : DataTypes.SMALLINT())
        .build();
  }

  private static TypeInformation<Row> rowType(String type) {
    return Types.ROW_NAMED(
        new String[] {"k", "v"},
        Types.INT,
        type.equals("BOOLEAN") ? Types.BOOLEAN : type.equals("TINYINT") ? Types.BYTE : Types.SHORT);
  }

  private static Row row(String type, RowKind kind, Integer key, Integer value) {
    Object typed =
        value == null
            ? null
            : type.equals("BOOLEAN")
                ? (Object) (value > 0)
                : type.equals("TINYINT") ? (Object) value.byteValue() : value.shortValue();
    return Row.ofKind(kind, key, typed);
  }

  private static List<Row> rows(String type) {
    int min = type.equals("TINYINT") ? Byte.MIN_VALUE : Short.MIN_VALUE;
    int max = type.equals("TINYINT") ? Byte.MAX_VALUE : Short.MAX_VALUE;
    return List.of(
        row(type, RowKind.INSERT, 0, min), row(type, RowKind.INSERT, 0, min),
        row(type, RowKind.INSERT, 0, null), row(type, RowKind.INSERT, 1, null),
        row(type, RowKind.INSERT, null, max), row(type, RowKind.UPDATE_BEFORE, 0, min),
        row(type, RowKind.UPDATE_AFTER, 0, max), row(type, RowKind.DELETE, 0, min),
        row(type, RowKind.DELETE, 1, null), row(type, RowKind.INSERT, 1, 7));
  }

  private static TableEnvironment environment(String type) {
    var env = METRICS.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = onePhase(StreamTableEnvironment.create(env));
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    table.createTemporaryView(
        "src",
        ((StreamTableEnvironment) table)
            .fromChangelogStream(
                tech.streamfusion.compat.FlinkTestSources.fromData(env, rows(type), rowType(type)),
                schema(type)));
    return table;
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,TINYINT",
    "COLLECT,SMALLINT",
    "ARRAY_AGG,TINYINT",
    "ARRAY_AGG,SMALLINT",
    "COLLECT,BOOLEAN",
    "ARRAY_AGG,BOOLEAN"
  })
  void primitiveElementsRetainValuesNullsAndChangelog(String function, String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    check(
        type,
        "SELECT k,"
            + function
            + "(v),"
            + function
            + "(DISTINCT v),"
            + function
            + "(v) FILTER (WHERE "
            + (type.equals("BOOLEAN") ? "v" : "v>0")
            + "),COUNT(*) FROM src GROUP BY k",
        "NativeJvmGroup");
    check(
        type,
        "SELECT k,CAST("
            + function
            + "(v) AS STRING),"
            + function
            + "(COALESCE(v,CAST("
            + (type.equals("BOOLEAN") ? "FALSE" : "0")
            + " AS "
            + type
            + "))) FROM src GROUP BY k",
        "NativeJvmGroup");
    if (function.equals("ARRAY_AGG"))
      check(
          type,
          "SELECT k,ARRAY_AGG(v) IGNORE NULLS,ARRAY_AGG(v) RESPECT NULLS FROM src GROUP BY k",
          "NativeJvmGroup");
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,TINYINT",
    "COLLECT,SMALLINT",
    "ARRAY_AGG,TINYINT",
    "ARRAY_AGG,SMALLINT",
    "COLLECT,BOOLEAN",
    "ARRAY_AGG,BOOLEAN"
  })
  void emptyGlobalAndGroupedResultsRetainResolvedTypes(String function, String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    check(type, "SELECT " + function + "(v),COUNT(*) FROM src WHERE k<0", "NativeFilter");
    check(type, "SELECT k," + function + "(v) FROM src WHERE k<0 GROUP BY k", "NativeFilter");
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"TINYINT", "SMALLINT", "BOOLEAN"})
  void nullablePrimitiveArraysRetainVerifiedRankAndUnnestConsumers(String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    check(
        type,
        "SELECT k,a FROM (SELECT k,a,ROW_NUMBER() OVER (ORDER BY k) AS rn FROM "
            + "(SELECT k,ARRAY_AGG(v) AS a FROM src GROUP BY k)) WHERE rn<=2",
        "NativeJvmGroup",
        "NativeColumnarTopN");
    check(
        type,
        "SELECT k,x FROM (SELECT k,ARRAY_AGG(v) AS a FROM src GROUP BY k) AS s "
            + "CROSS JOIN UNNEST(a) AS u(x)",
        "NativeJvmGroup",
        "NativeUnnest");
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"TINYINT", "SMALLINT", "BOOLEAN"})
  void updatingMultisetPayloadExtensionRemainsDeferred(String type) {
    NativeFailureParity.run(
            () -> environment(type),
            "SELECT k,m FROM (SELECT k,m,ROW_NUMBER() OVER (ORDER BY k) AS rn FROM "
                + "(SELECT k,COLLECT(v) AS m FROM src GROUP BY k)) WHERE rn<=2")
        .assertSuccess(NativeFailureParity.Route.FALLBACK);
  }

  private static void check(String type, String sql, String... consumed) {
    var previous =
        METRICS.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs = NativeFailureParity.run(() -> environment(type), sql);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertEquals(perKey(runs.host().rows()), perKey(runs.nativeRun().rows()));
    var work =
        METRICS.nativeOperators().stream()
            .filter(row -> !previous.contains(row.get("jobId")))
            .collect(java.util.stream.Collectors.toList());
    for (String operator : consumed) RecoveryExecutionMetrics.assertConsumed(work, operator);
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,TINYINT,hashmap",
    "COLLECT,SMALLINT,hashmap",
    "ARRAY_AGG,TINYINT,hashmap",
    "ARRAY_AGG,SMALLINT,hashmap",
    "COLLECT,BOOLEAN,hashmap",
    "ARRAY_AGG,BOOLEAN,hashmap",
    "COLLECT,TINYINT,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "COLLECT,SMALLINT,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,TINYINT,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,SMALLINT,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "COLLECT,BOOLEAN,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,BOOLEAN,tech.streamfusion.state.RocksDBNativeStateBackendFactory"
  })
  void primitiveValuesRestoreAcrossTwoNonemptyPrefixes(String function, String type, String backend)
      throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 48; i++)
      rows.add(row(type, RowKind.INSERT, i % 3, i % 5 == 0 ? null : i % 7));
    rows.add(type.equals("BOOLEAN") ? 0 : 17, row(type, RowKind.DELETE, 0, 99));
    for (int i = 0; i < 48; i++)
      rows.add(row(type, RowKind.DELETE, i % 3, i % 5 == 0 ? null : i % 7));
    rows.add(70, row(type, RowKind.INSERT, 0, 99));
    rows.addAll(rows(type));
    try (var recovery =
        new PortableSqlRecovery(backend, rows, rowType(type), schema(type), true, 16, 64)
            .withExecutionMetrics()) {
      String sql =
          "SELECT k,"
              + function
              + "(v),"
              + function
              + "(DISTINCT v),COUNT(*) FROM recovery_input GROUP BY k";
      var runs =
          NativeFailureParity.runRecovery(
              () -> onePhase(recovery.uninterrupted()), () -> onePhase(recovery.get()), sql);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertEquals(perKey(runs.host().rows()), perKey(runs.nativeRun().rows()));
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeJvmGroup");
      SharedFlinkCluster.assertNativeMemoryReleased("primitive-collection-recovery");
    }
  }

  private static TableEnvironment onePhase(TableEnvironment table) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    return table;
  }

  private static java.util.Map<Object, List<List<Object>>> perKey(List<List<Object>> rows) {
    var result = new java.util.HashMap<Object, List<List<Object>>>();
    for (var row : rows) result.computeIfAbsent(row.get(1), ignored -> new ArrayList<>()).add(row);
    return result;
  }
}
