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
  private static final List<String> ELEMENT_TYPES =
      List.of(
          "TINYINT",
          "SMALLINT",
          "INT",
          "BIGINT",
          "BOOLEAN",
          "DATE",
          "TIME",
          "FLOAT",
          "DOUBLE",
          "DECIMAL(8,2)",
          "DECIMAL(20,4)",
          "DECIMAL(38,18)");

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments>
      collectionSignatures() {
    return ELEMENT_TYPES.stream()
        .flatMap(
            type ->
                java.util.stream.Stream.of("COLLECT", "ARRAY_AGG")
                    .map(
                        function ->
                            org.junit.jupiter.params.provider.Arguments.of(function, type)));
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments>
      collectionRecoveryProfiles() {
    return collectionSignatures()
        .flatMap(
            signature ->
                java.util.stream.Stream.of(
                        "hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
                    .map(
                        backend ->
                            org.junit.jupiter.params.provider.Arguments.of(
                                signature.get()[0], signature.get()[1], backend)));
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments>
      nonNullSourceHintProfiles() {
    return collectionSignatures()
        .flatMap(
            signature ->
                java.util.stream.Stream.of(1, 3)
                    .map(
                        parallelism ->
                            org.junit.jupiter.params.provider.Arguments.of(
                                signature.get()[0], signature.get()[1], parallelism)));
  }

  static java.util.stream.Stream<String> collectionElementTypes() {
    return ELEMENT_TYPES.stream();
  }

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
    return schema(type, precision, notNull, false);
  }

  private static Schema schema(String type, int precision, boolean notNull, boolean notNullKey) {
    var value = DataTypes.of(type.equals("TIME") ? "TIME(" + precision + ")" : type);
    return Schema.newBuilder()
        .column("k", notNullKey ? DataTypes.INT().notNull() : DataTypes.INT())
        .column("v", notNull ? value.notNull() : value)
        .build();
  }

  private static TypeInformation<?> valueType(String type) {
    if (type.startsWith("DECIMAL")) return Types.BIG_DEC;
    switch (type) {
      case "FLOAT":
        return Types.FLOAT;
      case "DOUBLE":
        return Types.DOUBLE;
      case "INT":
        return Types.INT;
      case "BIGINT":
        return Types.LONG;
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
    if (type.startsWith("DECIMAL"))
      return new java.math.BigDecimal(
          java.math.BigInteger.valueOf(value),
          ((org.apache.flink.table.types.logical.DecimalType)
                  org.apache.flink.table.types.logical.utils.LogicalTypeParser.parse(
                      type, FlinkPrimitiveCollectionSqlHarnessTest.class.getClassLoader()))
              .getScale());
    switch (type) {
      case "FLOAT":
        return (float) value;
      case "DOUBLE":
        return (double) value;
      case "INT":
        return value;
      case "BIGINT":
        return (long) value;
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
    if (type.equals("FLOAT") || type.equals("DOUBLE")) {
      for (long bits :
          new long[] {
            0x8000000000000000L,
            0L,
            0x7ff8000000000001L,
            0xfff8000000000002L,
            0x7ff0000000000000L,
            0xfff0000000000000L,
            1L
          }) {
        Object edge =
            type.equals("FLOAT")
                ? (Object)
                    Float.intBitsToFloat(
                        bits == 0x7ff8000000000001L
                            ? 0x7fc00001
                            : bits == 0xfff8000000000002L
                                ? 0xffc00002
                                : Float.floatToRawIntBits((float) Double.longBitsToDouble(bits)))
                : Double.longBitsToDouble(bits);
        result.add(Row.ofKind(RowKind.INSERT, 3, edge));
        result.add(Row.ofKind(RowKind.INSERT, 3, edge));
        result.add(Row.ofKind(RowKind.DELETE, 3, edge));
      }
    }
    if (type.startsWith("DECIMAL")) {
      var decimal =
          (org.apache.flink.table.types.logical.DecimalType)
              org.apache.flink.table.types.logical.utils.LogicalTypeParser.parse(
                  type, FlinkPrimitiveCollectionSqlHarnessTest.class.getClassLoader());
      var largest =
          new java.math.BigDecimal(
              new java.math.BigInteger("9".repeat(decimal.getPrecision())), decimal.getScale());
      for (var edge :
          List.of(
              largest, largest.negate(), java.math.BigDecimal.ZERO.setScale(decimal.getScale()))) {
        result.add(Row.ofKind(RowKind.INSERT, 3, edge));
        result.add(Row.ofKind(RowKind.INSERT, 3, edge));
        result.add(Row.ofKind(RowKind.DELETE, 3, edge));
      }
    }
    return result;
  }

  private static TableEnvironment environment(String type) {
    return environment(type, 3);
  }

  private static TableEnvironment environment(String type, int precision) {
    return environment(type, precision, false);
  }

  private static TableEnvironment environment(String type, int precision, boolean notNull) {
    return environment(type, precision, notNull, 1);
  }

  private static TableEnvironment environment(
      String type, int precision, boolean notNull, int parallelism) {
    return environment(type, precision, notNull, parallelism, false);
  }

  private static TableEnvironment environment(
      String type, int precision, boolean notNull, int parallelism, boolean notNullKey) {
    var env = METRICS.environment(new Configuration(), parallelism, 0);
    env.getConfig().enableObjectReuse();
    var table = onePhase(StreamTableEnvironment.create(env));
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    var inputRows = rows(type);
    if (notNull) java.util.Collections.rotate(inputRows, -4);
    table.createTemporaryView(
        "src",
        ((StreamTableEnvironment) table)
            .fromChangelogStream(
                tech.streamfusion.compat.FlinkTestSources.fromData(env, inputRows, rowType(type)),
                schema(type, precision, notNull, notNullKey)));
    return table;
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("nonNullSourceHintProfiles")
  void nonNullCollectionSourceHintsRetainReleasedNullSlots(
      String function, String type, int parallelism) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    check(
        () -> environment(type, 3, true, parallelism),
        "SELECT k," + function + "(v)," + function + "(DISTINCT v),COUNT(*) FROM src GROUP BY k",
        "NativeColumnarGroupAggregate");
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"DATE", "TIME"})
  void nonNullGroupingKeyHintsPreserveSinkConstraintFailures(String type) {
    var runs =
        NativeFailureParity.run(
            () -> environment(type, 3, true, 3, true),
            "SELECT k,COLLECT(v),COUNT(*) FROM src GROUP BY k");
    runs.assertCompetingFailures(
        error -> error.getMessage().contains("Column 'k' is NOT NULL"),
        NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().rootCause().getClass(), runs.nativeRun().rootCause().getClass());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"DATE", "TIME"})
  void nonNullTemporalHintsRetainInputCalcSemantics(String type) {
    String threshold = type.equals("DATE") ? "DATE '1970-01-01'" : "TIME '00:00:00'";
    NativeFailureParity.run(
            () -> environment(type, 3, true),
            "SELECT k,COLLECT(v),COUNT(*) FROM src WHERE v>=" + threshold + " GROUP BY k")
        .assertSuccess(NativeFailureParity.Route.FALLBACK);
    check(
        () -> environment(type, 3, true),
        "SELECT k,COLLECT(v),COUNT(*) FROM src WHERE k>=0 GROUP BY k",
        "NativeColumnarGroupAggregate");
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
  @org.junit.jupiter.params.provider.MethodSource("collectionSignatures")
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
    check(type, nonNull, "NativeColumnarGroupAggregate");
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
  @org.junit.jupiter.params.provider.MethodSource("collectionSignatures")
  void emptyGlobalAndGroupedResultsRetainResolvedTypes(String function, String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    check(type, "SELECT " + function + "(v),COUNT(*) FROM src WHERE k<0", "NativeFilter");
    check(type, "SELECT k," + function + "(v) FROM src WHERE k<0 GROUP BY k", "NativeFilter");
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("collectionElementTypes")
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
  @org.junit.jupiter.params.provider.MethodSource("collectionElementTypes")
  void updatingMultisetPayloadExtensionRemainsDeferred(String type) {
    NativeFailureParity.run(
            () -> environment(type),
            "SELECT k,m FROM (SELECT k,m,ROW_NUMBER() OVER (ORDER BY k) AS rn FROM "
                + "(SELECT k,COLLECT(v) AS m FROM src GROUP BY k)) WHERE rn<=2")
        .assertSuccess(NativeFailureParity.Route.FALLBACK);
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"DATE", "TIME"})
  void nonNullTemporalArraysKeepCopyCategoriesThroughMixedConsumers(String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    String zero = type.equals("DATE") ? "DATE '1970-01-01'" : "TIME '00:00:00'";
    String input =
        "(SELECT k,ARRAY_AGG(COALESCE(v,"
            + zero
            + ")) AS a,"
            + "ARRAY_AGG(COALESCE(k,0)) AS b,ARRAY_AGG(k) AS c FROM src GROUP BY k)";
    for (String projection :
        List.of(
            "k,a,b,c,CAST(a AS STRING)",
            "k,CAST(a AS STRING),a,a",
            "k,ARRAY_REVERSE(a),ARRAY_DISTINCT(a),ARRAY_SLICE(a,1,2)",
            "k,CASE WHEN k=0 THEN a ELSE ARRAY_REVERSE(a) END"))
      check(
          type,
          "SELECT " + projection + " FROM " + input,
          "NativeColumnarGroupAggregate",
          "NativeCalc");
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"DATE", "TIME"})
  void nonNullTemporalConsumersPreserveActualNullSlotBehavior(String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    check(
        () -> environment(type, 3, true),
        "SELECT k,a,CAST(a AS STRING),ARRAY_REVERSE(a),ARRAY_SLICE(a,1,2) "
            + "FROM (SELECT k,ARRAY_AGG(v) AS a FROM src GROUP BY k)",
        "NativeColumnarGroupAggregate",
        "NativeCalc");
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"DATE", "TIME"})
  void nonNullTemporalArraysRetainUnverifiedStateAndUnnestGuards(String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    String zero = type.equals("DATE") ? "DATE '1970-01-01'" : "TIME '00:00:00'";
    String input = "(SELECT k,ARRAY_AGG(COALESCE(v," + zero + ")) AS a FROM src GROUP BY k)";
    for (String sql :
        List.of(
            "SELECT k,x FROM " + input + " AS s CROSS JOIN UNNEST(a) AS u(x)",
            "SELECT k,a FROM (SELECT k,a,ROW_NUMBER() OVER (ORDER BY k) AS rn FROM "
                + input
                + ") WHERE rn<=2"))
      NativeFailureParity.run(() -> environment(type), sql)
          .assertSuccess(NativeFailureParity.Route.FALLBACK);
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"DATE", "TIME"})
  void nonNullTemporalArraysRetainTheExplicitLegacyHandler(String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    String property = "streamfusion.experimental.rustCollectionAggregates";
    String previous = System.getProperty(property);
    try {
      System.setProperty(property, "false");
      check(
          () -> environment(type, 3, true),
          "SELECT k,ARRAY_AGG(v),CAST(ARRAY_AGG(v) AS STRING) FROM src GROUP BY k",
          "NativeJvmGroup");
      String zero = type.equals("DATE") ? "DATE '1970-01-01'" : "TIME '00:00:00'";
      check(
          type,
          "SELECT CAST(k AS STRING),ARRAY_AGG(COALESCE(v,"
              + zero
              + ")),CAST(ARRAY_AGG(COALESCE(v,"
              + zero
              + ")) AS STRING),ARRAY_AGG(COALESCE(k,0)),ARRAY_AGG(k),COUNT(*) "
              + "FROM src GROUP BY k",
          "NativeJvmGroup");
    } finally {
      if (previous == null) System.clearProperty(property);
      else System.setProperty(property, previous);
    }
  }

  @ParameterizedTest
  @CsvSource({"COLLECT,DATE", "COLLECT,TIME", "ARRAY_AGG,DATE", "ARRAY_AGG,TIME"})
  void nonNullTemporalMaterializationRetainsHostValuesAndFailures(String function, String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    String sql =
        function.equals("ARRAY_AGG")
            ? "SELECT CAST(k AS STRING),ARRAY_AGG(v),CAST(ARRAY_AGG(v) AS STRING),"
                + "ARRAY_AGG(COALESCE(k,0)),ARRAY_AGG(k),COUNT(*) FROM src GROUP BY k"
            : "SELECT CAST(k AS STRING),COLLECT(v),COLLECT(COALESCE(k,0)),COUNT(*) FROM src GROUP"
                + " BY k";
    var runs = NativeFailureParity.run(() -> environment(type, 3, true), sql);
    if (type.equals("TIME"))
      runs.assertFailure(
          java.time.DateTimeException.class,
          NativeFailureParity.Phase.COLLECTION,
          NativeFailureParity.Route.FALLBACK);
    else runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
    assertTrue(
        runs.nativeRun().fallbackReasons().stream()
            .anyMatch(reason -> reason.contains("collection input materialization")),
        runs.toString());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("collectionSignatures")
  void collectionInputMaterializationPreservesTheReleasedPhysicalGetters(
      String function, String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var runs =
        NativeFailureParity.run(
            () -> environment(type, 3, true),
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
  @org.junit.jupiter.params.provider.MethodSource("decimalBoundarySignatures")
  void decimalCollectionMetadataKeepsPrecisionAndScaleBoundaries(String function, String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    check(
        type,
        "SELECT k," + function + "(v)," + function + "(DISTINCT v),COUNT(*) FROM src GROUP BY k",
        "NativeColumnarGroupAggregate");
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments>
      decimalBoundarySignatures() {
    return java.util.stream.Stream.of(
            "DECIMAL(1,1)", "DECIMAL(18,18)", "DECIMAL(19,0)", "DECIMAL(38,0)", "DECIMAL(38,38)")
        .flatMap(
            type ->
                java.util.stream.Stream.of("COLLECT", "ARRAY_AGG")
                    .map(
                        function ->
                            org.junit.jupiter.params.provider.Arguments.of(function, type)));
  }

  @org.apache.flink.table.annotation.FunctionHint(
      input = @org.apache.flink.table.annotation.DataTypeHint("ARRAY<FLOAT>"),
      output = @org.apache.flink.table.annotation.DataTypeHint("ARRAY<BIGINT>"))
  public static class FloatArrayBits extends org.apache.flink.table.functions.ScalarFunction {
    public Long[] eval(Float[] input) {
      if (input == null) return null;
      Long[] bits = new Long[input.length];
      for (int i = 0; i < input.length; i++)
        bits[i] =
            input[i] == null ? null : Integer.toUnsignedLong(Float.floatToRawIntBits(input[i]));
      return bits;
    }
  }

  @org.apache.flink.table.annotation.FunctionHint(
      input = @org.apache.flink.table.annotation.DataTypeHint("ARRAY<DOUBLE>"),
      output = @org.apache.flink.table.annotation.DataTypeHint("ARRAY<BIGINT>"))
  public static class DoubleArrayBits extends org.apache.flink.table.functions.ScalarFunction {
    public Long[] eval(Double[] input) {
      if (input == null) return null;
      Long[] bits = new Long[input.length];
      for (int i = 0; i < input.length; i++)
        bits[i] = input[i] == null ? null : Double.doubleToRawLongBits(input[i]);
      return bits;
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"FLOAT", "DOUBLE"})
  void floatingCollectionPayloadBitsSurviveTheRealCallbackBoundary(String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    check(
        () -> {
          var table = environment(type);
          table.createTemporarySystemFunction(
              "collection_bits",
              type.equals("FLOAT") ? FloatArrayBits.class : DoubleArrayBits.class);
          return table;
        },
        "SELECT k,collection_bits(ARRAY_AGG(v)),collection_bits(ARRAY_AGG(DISTINCT v)) "
            + "FROM src GROUP BY k",
        "NativeColumnarGroupAggregate",
        "NativeCalc");
  }

  @org.apache.flink.table.annotation.FunctionHint(
      input = @org.apache.flink.table.annotation.DataTypeHint("ARRAY<DATE NOT NULL>"),
      output = @org.apache.flink.table.annotation.DataTypeHint("ARRAY<DATE NOT NULL>"))
  public static class RetainedDates extends org.apache.flink.table.functions.ScalarFunction {
    private transient java.time.LocalDate[] previous;

    public java.time.LocalDate[] eval(java.time.LocalDate[] value) {
      var result = previous;
      previous = value;
      return result;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  @org.apache.flink.table.annotation.FunctionHint(
      input = @org.apache.flink.table.annotation.DataTypeHint("ARRAY<TIME(0) NOT NULL>"),
      output = @org.apache.flink.table.annotation.DataTypeHint("ARRAY<TIME(0) NOT NULL>"))
  public static class RetainedTimes extends org.apache.flink.table.functions.ScalarFunction {
    private transient java.time.LocalTime[] previous;

    public java.time.LocalTime[] eval(java.time.LocalTime[] value) {
      var result = previous;
      previous = value;
      return result;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  @ParameterizedTest
  @CsvSource({
    "DATE,false,false", "DATE,true,false", "TIME,false,false", "TIME,true,false",
    "DATE,false,true", "DATE,true,true", "TIME,false,true", "TIME,true,true"
  })
  void temporalArrayCallbacksKeepOwnedInputsAndSharedState(
      String type, boolean repeated, boolean nullSourceSlots) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    String zero = type.equals("DATE") ? "DATE '1970-01-01'" : "TIME '00:00:00'";
    java.util.function.Supplier<TableEnvironment> factory =
        () -> {
          var table = environment(type, 3, nullSourceSlots);
          table.createTemporarySystemFunction(
              "retain_temporal", type.equals("DATE") ? RetainedDates.class : RetainedTimes.class);
          return table;
        };
    String projection = repeated ? "retain_temporal(a),retain_temporal(a)" : "retain_temporal(a)";
    String argument = nullSourceSlots ? "v" : "COALESCE(v," + zero + ")";
    check(
        factory,
        "SELECT k,"
            + projection
            + " FROM (SELECT k,ARRAY_AGG("
            + argument
            + ") AS a FROM src GROUP BY k)",
        "NativeColumnarGroupAggregate",
        "NativeCalc");
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
  @org.junit.jupiter.params.provider.MethodSource("collectionRecoveryProfiles")
  void primitiveValuesRestoreAcrossTwoNonemptyPrefixes(String function, String type, String backend)
      throws Exception {
    restore(function, type, backend, false);
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,DATE,hashmap",
    "COLLECT,TIME,hashmap",
    "ARRAY_AGG,DATE,hashmap",
    "ARRAY_AGG,TIME,hashmap",
    "COLLECT,DATE,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "COLLECT,TIME,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,DATE,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,TIME,tech.streamfusion.state.RocksDBNativeStateBackendFactory"
  })
  void nonNullTemporalHintsRestoreNullSlotsTwice(String function, String type, String backend)
      throws Exception {
    restore(function, type, backend, true);
  }

  private static void restore(String function, String type, String backend, boolean notNull)
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
        new PortableSqlRecovery(
                backend, rows, rowType(type), schema(type, 3, notNull), true, 16, 64)
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
