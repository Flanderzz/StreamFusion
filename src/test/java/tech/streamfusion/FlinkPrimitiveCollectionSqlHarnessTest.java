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
    return schema(type, 3);
  }

  private static Schema schema(String type, int precision) {
    return schema(type, precision, false);
  }

  private static Schema schema(String type, int precision, boolean notNull) {
    var value = DataTypes.of(type.equals("TIME") ? "TIME(" + precision + ")" : type);
    return Schema.newBuilder()
        .column("k", DataTypes.INT())
        .column("v", notNull ? value.notNull() : value)
        .build();
  }

  private static TypeInformation<?> valueType(String type) {
    switch (type) {
      case "DATE":
        return Types.LOCAL_DATE;
      case "TIME":
        return Types.LOCAL_TIME;
      case "BOOLEAN":
        return Types.BOOLEAN;
      case "TINYINT":
        return Types.BYTE;
      case "SMALLINT":
        return Types.SHORT;
      default:
        throw new IllegalArgumentException(type);
    }
  }

  private static TypeInformation<Row> rowType(String type) {
    return Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, valueType(type));
  }

  private static Row row(String type, RowKind kind, Integer key, Integer value) {
    return Row.ofKind(kind, key, value == null ? null : value(type, value));
  }

  private static Object value(String type, int value) {
    switch (type) {
      case "DATE":
        return java.time.LocalDate.ofEpochDay(value);
      case "TIME":
        return java.time.LocalTime.ofNanoOfDay(Math.floorMod(value, 86_400_000) * 1_000_000L);
      case "BOOLEAN":
        return value > 0;
      case "TINYINT":
        return (byte) value;
      case "SMALLINT":
        return (short) value;
      default:
        throw new IllegalArgumentException(type);
    }
  }

  private static List<Row> rows(String type) {
    int min =
        type.equals("DATE")
            ? -719162
            : type.equals("TIME") ? 0 : type.equals("TINYINT") ? Byte.MIN_VALUE : Short.MIN_VALUE;
    int max =
        type.equals("DATE")
            ? 2932896
            : type.equals("TIME")
                ? 86_399_999
                : type.equals("TINYINT") ? Byte.MAX_VALUE : Short.MAX_VALUE;
    var result =
        new ArrayList<>(
            List.of(
                row(type, RowKind.INSERT, 0, min), row(type, RowKind.INSERT, 0, min),
                row(type, RowKind.INSERT, 0, null), row(type, RowKind.INSERT, 1, null),
                row(type, RowKind.INSERT, null, max), row(type, RowKind.UPDATE_BEFORE, 0, min),
                row(type, RowKind.UPDATE_AFTER, 0, max), row(type, RowKind.DELETE, 0, min),
                row(type, RowKind.DELETE, 1, null), row(type, RowKind.INSERT, 1, 7)));
    if (type.equals("DATE"))
      for (var date :
          List.of(
              java.time.LocalDate.of(1900, 2, 28),
              java.time.LocalDate.of(1900, 3, 1),
              java.time.LocalDate.of(2000, 2, 29),
              java.time.LocalDate.of(1970, 1, 1)))
        result.add(row(type, RowKind.INSERT, 2, (int) date.toEpochDay()));
    if (type.equals("TIME"))
      for (int millis : new int[] {1, 999, 1000, 12_345_678, 86_399_999})
        result.add(row(type, RowKind.INSERT, 2, millis));
    return result;
  }

  private static TableEnvironment environment(String type) {
    return environment(type, 3);
  }

  private static TableEnvironment environment(String type, int precision) {
    return environment(type, precision, false);
  }

  private static TableEnvironment environment(String type, int precision, boolean notNull) {
    var env = METRICS.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = onePhase(StreamTableEnvironment.create(env));
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    table.createTemporaryView(
        "src",
        ((StreamTableEnvironment) table)
            .fromChangelogStream(
                tech.streamfusion.compat.FlinkTestSources.fromData(env, rows(type), rowType(type)),
                schema(type, precision, notNull)));
    return table;
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"DATE", "TIME"})
  void nonNullTemporalSourceHintsRetainReleasedNullSlots(String type) {
    NativeFailureParity.run(
            () -> environment(type, 3, true),
            "SELECT k,COLLECT(v),COLLECT(DISTINCT v),COUNT(*) FROM src GROUP BY k")
        .assertSuccess(NativeFailureParity.Route.FALLBACK);
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,0",
    "COLLECT,1",
    "COLLECT,2",
    "COLLECT,3",
    "ARRAY_AGG,0",
    "ARRAY_AGG,1",
    "ARRAY_AGG,2",
    "ARRAY_AGG,3"
  })
  void timeDeclarationsKeepRuntimeMilliseconds(String function, int precision) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    check(
        () -> environment("TIME", precision),
        "SELECT k," + function + "(v)," + function + "(DISTINCT v),COUNT(*) FROM src GROUP BY k",
        "NativeColumnarGroupAggregate");
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,TINYINT",
    "COLLECT,SMALLINT",
    "ARRAY_AGG,TINYINT",
    "ARRAY_AGG,SMALLINT",
    "COLLECT,BOOLEAN",
    "ARRAY_AGG,BOOLEAN",
    "COLLECT,DATE",
    "ARRAY_AGG,DATE",
    "COLLECT,TIME",
    "ARRAY_AGG,TIME"
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
            + (type.equals("DATE")
                ? "v>=DATE '1970-01-01'"
                : type.equals("TIME") ? "v>=TIME '12:00:00'" : type.equals("BOOLEAN") ? "v" : "v>0")
            + "),COUNT(*) FROM src GROUP BY k",
        "NativeColumnarGroupAggregate");
    String nonNull =
        "SELECT k,CAST("
            + function
            + "(v) AS STRING),"
            + function
            + "(COALESCE(v,CAST("
            + (type.equals("DATE")
                ? "DATE '1970-01-01'"
                : type.equals("TIME") ? "TIME '00:00:00'" : type.equals("BOOLEAN") ? "FALSE" : "0")
            + " AS "
            + type
            + "))) FROM src GROUP BY k";
    if (type.equals("DATE") || type.equals("TIME"))
      NativeFailureParity.run(() -> environment(type), nonNull)
          .assertSuccess(NativeFailureParity.Route.FALLBACK);
    else check(type, nonNull, "NativeColumnarGroupAggregate");
    if (type.equals("DATE") || type.equals("TIME"))
      check(
          type,
          "SELECT k,CAST(" + function + "(v) AS STRING) FROM src GROUP BY k",
          "NativeColumnarGroupAggregate",
          "NativeCalc");
    if (function.equals("ARRAY_AGG"))
      check(
          type,
          "SELECT k,ARRAY_AGG(v) IGNORE NULLS,ARRAY_AGG(v) RESPECT NULLS FROM src GROUP BY k",
          "NativeColumnarGroupAggregate");
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,TINYINT",
    "COLLECT,SMALLINT",
    "ARRAY_AGG,TINYINT",
    "ARRAY_AGG,SMALLINT",
    "COLLECT,BOOLEAN",
    "ARRAY_AGG,BOOLEAN",
    "COLLECT,DATE",
    "ARRAY_AGG,DATE",
    "COLLECT,TIME",
    "ARRAY_AGG,TIME"
  })
  void emptyGlobalAndGroupedResultsRetainResolvedTypes(String function, String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    check(type, "SELECT " + function + "(v),COUNT(*) FROM src WHERE k<0", "NativeFilter");
    check(type, "SELECT k," + function + "(v) FROM src WHERE k<0 GROUP BY k", "NativeFilter");
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"TINYINT", "SMALLINT", "BOOLEAN", "DATE", "TIME"})
  void nullablePrimitiveArraysRetainVerifiedRankAndUnnestConsumers(String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    check(
        type,
        "SELECT k,a FROM (SELECT k,a,ROW_NUMBER() OVER (ORDER BY k) AS rn FROM "
            + "(SELECT k,ARRAY_AGG(v) AS a FROM src GROUP BY k)) WHERE rn<=2",
        "NativeColumnarGroupAggregate",
        "NativeColumnarTopN");
    check(
        type,
        "SELECT k,x FROM (SELECT k,ARRAY_AGG(v) AS a FROM src GROUP BY k) AS s "
            + "CROSS JOIN UNNEST(a) AS u(x)",
        "NativeColumnarGroupAggregate",
        "NativeUnnest");
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"TINYINT", "SMALLINT", "BOOLEAN", "DATE", "TIME"})
  void updatingMultisetPayloadExtensionRemainsDeferred(String type) {
    NativeFailureParity.run(
            () -> environment(type),
            "SELECT k,m FROM (SELECT k,m,ROW_NUMBER() OVER (ORDER BY k) AS rn FROM "
                + "(SELECT k,COLLECT(v) AS m FROM src GROUP BY k)) WHERE rn<=2")
        .assertSuccess(NativeFailureParity.Route.FALLBACK);
  }

  private static void check(String type, String sql, String... consumed) {
    check(() -> environment(type), sql, consumed);
  }

  private static void check(
      java.util.function.Supplier<TableEnvironment> environment, String sql, String... consumed) {
    var previous =
        METRICS.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs = NativeFailureParity.run(environment, sql);
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
    "COLLECT,DATE,hashmap",
    "ARRAY_AGG,DATE,hashmap",
    "COLLECT,TIME,hashmap",
    "ARRAY_AGG,TIME,hashmap",
    "COLLECT,TINYINT,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "COLLECT,SMALLINT,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,TINYINT,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,SMALLINT,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "COLLECT,BOOLEAN,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,BOOLEAN,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "COLLECT,DATE,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,DATE,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "COLLECT,TIME,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,TIME,tech.streamfusion.state.RocksDBNativeStateBackendFactory"
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
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
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
