package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkJsonReturningSqlHarnessTest {
  @Test
  void booleanValuesAndDefaultsMatchFlink() throws Exception {
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                null, "true", "false", "null", "{}", "[]", "invalid"),
        "SELECT id, JSON_VALUE(s, '$' RETURNING BOOLEAN), JSON_VALUE(s, 'lax $' RETURNING BOOLEAN"
            + " DEFAULT TRUE ON EMPTY DEFAULT FALSE ON ERROR) FROM inputs");
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows(null, "true", "false", "null"),
        "SELECT id FROM inputs WHERE JSON_VALUE(s, '$' RETURNING BOOLEAN"
            + " DEFAULT FALSE ON EMPTY DEFAULT FALSE ON ERROR)");
  }

  @Test
  void integerValuesBoundsAndDefaultsMatchFlink() throws Exception {
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                null, "0", "-0", "2147483647", "-2147483648", "null", "{}", "invalid"),
        "SELECT id, JSON_VALUE(s, '$' RETURNING INTEGER), JSON_VALUE(s, 'lax $' RETURNING INTEGER"
            + " DEFAULT 7 ON EMPTY DEFAULT -9 ON ERROR) FROM inputs");
  }

  @Test
  void doublesPreserveRoundingZerosOverflowAndUnderflow() throws Exception {
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                null,
                "null",
                "{}",
                "invalid",
                "-0.0",
                "-0e10",
                "0.000e-300",
                "1.0",
                "1e3",
                "1.234567890123456789",
                "1e309",
                "-1e309",
                "1e-400",
                "-1e-400",
                "2.2250738585072014e-308",
                "4.9406564584124654e-324",
                "1.00000000000000011102230246251565404236316680908203125",
                "1e2147483647",
                "1e2147483648",
                "1e-2147483648"),
        "SELECT id, JSON_VALUE(s, '$' RETURNING DOUBLE) FROM inputs");
  }

  @ParameterizedTest
  @CsvSource({
    "BOOLEAN,1",
    "BOOLEAN,\"true\"",
    "INTEGER,1.0",
    "INTEGER,2147483648",
    "INTEGER,-2147483649",
    "INTEGER,\"12\"",
    "DOUBLE,1",
    "DOUBLE,\"1.0\""
  })
  void scalarTypeMismatchIsNotCaughtByNullOnError(String type, String document) {
    JsonFunctionTestInputs.assertFails(
        document,
        "JSON_VALUE(s, '$' RETURNING " + type + " NULL ON ERROR)",
        "JSON_VALUE RETURNING " + type);
  }

  @ParameterizedTest
  @ValueSource(strings = {"BOOLEAN", "INTEGER", "DOUBLE"})
  void errorOnEmptyStillFailsOutsideOnError(String type) {
    JsonFunctionTestInputs.assertFails(
        "{}",
        "JSON_VALUE(s, 'lax $.a' RETURNING " + type + " ERROR ON EMPTY NULL ON ERROR)",
        "JSON_VALUE EMPTY");
  }

  @Test
  void typedDefaultsAndUnsupportedDefaultsAreExplicitlyGated() throws Exception {
    String[] expressions = {
      "JSON_VALUE(s, '$' RETURNING DOUBLE DEFAULT 0.5 ON ERROR)",
      "JSON_VALUE(s, '$' RETURNING DOUBLE DEFAULT CAST(0.5 AS DOUBLE) ON ERROR)",
      "JSON_VALUE(s, '$' RETURNING INTEGER DEFAULT CAST(7 AS BIGINT) ON ERROR)",
      "JSON_VALUE(s, '$' RETURNING INTEGER DEFAULT CAST(NULL AS INT) ON ERROR)",
      "JSON_VALUE(s, '$' RETURNING BOOLEAN DEFAULT 'true' ON ERROR)"
    };
    for (String expression : expressions) {
      String plan =
          NativePlanner.explain(
              TextTimeFunctionTestInputs.textRows("1", null),
              "SELECT " + expression + " FROM inputs");
      assertFalse(plan.contains("NativeCalc"), plan);
      assertTrue(plan.contains("DEFAULT"), plan);
    }
  }

  @Test
  void nullableBooleanCompositionPreservesFlinkBoxedNullFailures() {
    String[] expressions = {
      "JSON_VALUE(s, '$' RETURNING BOOLEAN) IS TRUE",
      "JSON_VALUE(s, '$' RETURNING BOOLEAN) IS FALSE",
      "CASE WHEN JSON_VALUE(s, '$' RETURNING BOOLEAN) THEN 1 ELSE 0 END"
    };
    for (String expression : expressions) {
      assertFalse(
          NativePlanner.explain(
                  TextTimeFunctionTestInputs.textRows("null"),
                  "SELECT " + expression + " FROM inputs")
              .contains("NativeCalc"));
    }
    String sql = "SELECT id FROM inputs WHERE JSON_VALUE(s, '$' RETURNING BOOLEAN)";
    String plan = NativePlanner.explain(TextTimeFunctionTestInputs.textRows("null"), sql);
    assertFalse(plan.contains("NativeCalc"), plan);
    assertFalse(plan.contains("NativeFilter"), plan);
    assertTrue(plan.contains("direct projection"), plan);
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      var tables = TextTimeFunctionTestInputs.textRows("null");
      if (nativeEnabled) {
        NativePlanner.install(tables);
      }
      Exception failure =
          org.junit.jupiter.api.Assertions.assertThrows(
              Exception.class,
              () -> {
                try (var rows = tables.executeSql(sql).collect()) {
                  while (rows.hasNext()) {
                    rows.next();
                  }
                }
              });
      StringBuilder causes = new StringBuilder();
      for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
        causes.append(cause);
      }
      assertTrue(causes.toString().contains("NullPointerException"), causes.toString());
    }
  }

  @Test
  void extractedIntegerComposesWithNativeGrouping() throws Exception {
    String sql =
        "SELECT JSON_VALUE(s, '$.n' RETURNING INTEGER), COUNT(*) FROM inputs "
            + "GROUP BY JSON_VALUE(s, '$.n' RETURNING INTEGER)";
    String[] documents = {"{\"n\":1}", "{\"n\":2}", "{\"n\":1}", "{}", null};
    String plan = NativePlanner.explain(TextTimeFunctionTestInputs.textRows(documents), sql);
    assertTrue(plan.contains("NativeCalc"), plan);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    NativeParity.assertParity(() -> TextTimeFunctionTestInputs.textRows(documents), sql);
  }

  @ParameterizedTest
  @ValueSource(strings = {"id = 1 OR", "id = 0 AND"})
  void typedConversionsKeepFlinkLogicalShortCircuit(String prefix) throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> TextTimeFunctionTestInputs.textRows("1", "\"unused\""),
        "SELECT id, " + prefix + " JSON_VALUE(s, '$' RETURNING INTEGER) > 0 FROM inputs",
        "row short-circuiting");
  }

  @Test
  void caseEvaluatesOnlyTheSelectedConversion() throws Exception {
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows("1", "\"unused\""),
        "SELECT id, CASE WHEN id = 0 THEN JSON_VALUE(s, '$' RETURNING INTEGER) ELSE 0 END FROM"
            + " inputs");
  }
}
