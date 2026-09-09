package tech.streamfusion;

import java.util.function.Supplier;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;

class FlinkJsonValueSqlHarnessTest {
  private static void assertParity(Supplier<TableEnvironment> input, String sql) throws Exception {
    NativeParity.assertParity(JsonFunctionTestInputs.nativeInput("JSON_VALUE", input), sql);
  }

  private static void assertFallback(Supplier<TableEnvironment> input, String sql)
      throws Exception {
    NativeParity.assertFallback(JsonFunctionTestInputs.nativeInput("JSON_VALUE", input), sql);
  }

  @Test
  void strictDefaultPreservesFlinkUntilOptedIn() throws Exception {
    NativeParity.assertFallbackReasonContains(
        JsonFunctionTestInputs::documents,
        "SELECT id, JSON_VALUE(s, '$.a') FROM inputs",
        "allowIncompatible");
  }

  @Test
  void strictAndLaxDistinguishMissingNullContainersAndErrors() throws Exception {
    assertParity(
        JsonFunctionTestInputs::documents,
        "SELECT id, JSON_VALUE(s, '$.a'), "
            + "JSON_VALUE(s, 'lax $.a' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), "
            + "JSON_VALUE(s, 'strict $.a' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), "
            + "JSON_VALUE(s, '$'), JSON_VALUE(s, 'lax $') FROM inputs");
  }

  @Test
  void nestedMembersQuotedMembersAndArrayIndexes() throws Exception {
    assertParity(
        JsonFunctionTestInputs::documents,
        "SELECT id, JSON_VALUE(s, '$.a.b[1]'), JSON_VALUE(s, '$[1].a'), "
            + "JSON_VALUE(s, '$[''a b'']'), JSON_VALUE(s, ' LaX $.a'), "
            + "JSON_VALUE(s, '$.a' RETURNING VARCHAR(3)) FROM inputs");
  }

  @Test
  void wideDocumentsKeepDuplicateKeysAndUnselectedFieldValidation() throws Exception {
    assertParity(
        JsonFunctionTestInputs::wideDocuments,
        "SELECT id, JSON_VALUE(s, '$.a'), "
            + "JSON_VALUE(s, 'lax $.a' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), "
            + "JSON_VALUE(s, '$.a[1].b'), JSON_VALUE(s, '$.field63') FROM inputs");
  }

  @Test
  void numbersKeepJacksonBigDecimalAndBigIntegerText() throws Exception {
    assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                "-0",
                "-0.0",
                "1.2300",
                "1e3",
                "100e-1",
                "0.000001",
                "0.0000001",
                "92233720368547758081234567890",
                "1e2147483647",
                "0e999999999",
                "1e2147483648",
                "1e-2147483648",
                "0.1e-2147483647",
                "1e+00000000000000000000001",
                "1".repeat(1001),
                "{\"a\":1,\"bad\":1e2147483648}"),
        "SELECT id, JSON_VALUE(s, '$'), JSON_VALUE(s, 'lax $' DEFAULT 'empty' ON EMPTY), "
            + "JSON_VALUE(s, '$.a' DEFAULT 'error' ON ERROR) FROM inputs");
  }

  @Test
  void composesWithPredicatesAndDownstreamGrouping() throws Exception {
    String sql =
        "SELECT JSON_VALUE(s, '$.a'), COUNT(*) FROM inputs "
            + "WHERE JSON_VALUE(s, '$.a') IS NOT NULL GROUP BY JSON_VALUE(s, '$.a')";
    String plan =
        tech.streamfusion.planner.NativePlanner.explain(
            JsonFunctionTestInputs.nativeInput("JSON_VALUE", JsonFunctionTestInputs::documents)
                .get(),
            sql);
    org.junit.jupiter.api.Assertions.assertTrue(plan.contains("NativeCalc"), plan);
    org.junit.jupiter.api.Assertions.assertTrue(
        plan.contains("NativeColumnarGroupAggregate"), plan);
    assertParity(JsonFunctionTestInputs::documents, sql);
  }

  @Test
  void jacksonNumericParserSwitchAndConstraints() throws Exception {
    assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                "1." + "0".repeat(490) + "e2147483648",
                "1." + "0".repeat(480) + "e2147483648",
                "-" + "1".repeat(1000),
                "1." + "1".repeat(999),
                "[1." + "1".repeat(1000) + "]",
                "[0." + "1".repeat(1000) + "]",
                "true\u007f",
                "null\u0301",
                "null\u0870",
                "true\ud83d\ude00"),
        "SELECT id, JSON_VALUE(s, '$' DEFAULT 'error' ON ERROR), JSON_VALUE(s, 'lax $' DEFAULT"
            + " 'empty' ON EMPTY DEFAULT 'error' ON ERROR) FROM inputs");
  }

  @Test
  void nullAndNonCharacterDefaultsFallBack() throws Exception {
    assertFallback(
        JsonFunctionTestInputs::documents,
        "SELECT id, JSON_VALUE(s, '$.a' DEFAULT CAST(NULL AS STRING) ON ERROR) FROM inputs");
    assertFallback(
        JsonFunctionTestInputs::documents,
        "SELECT id, JSON_VALUE(s, '$.a' DEFAULT 12 ON ERROR) FROM inputs");
  }

  @Test
  void emptyErrorIsNotSwallowedByOnErrorDefault() {
    JsonFunctionTestInputs.assertFails(
        "{}",
        "JSON_VALUE(s, 'lax $.a' ERROR ON EMPTY DEFAULT 'error' ON ERROR)",
        "JSON_VALUE EMPTY");
    JsonFunctionTestInputs.assertFails(
        "{\"a\":null}", "JSON_VALUE(s, 'strict $.a' ERROR ON ERROR)", "JSON_VALUE ERROR");
  }

  @Test
  void unverifiedPathsAndReturningTypesFallBack() throws Exception {
    assertFallback(
        JsonFunctionTestInputs::documents, "SELECT id, JSON_VALUE(s, '$.*') FROM inputs");
    assertFallback(
        () -> TextTimeFunctionTestInputs.textRows("{\"a\":12}", "{}", null),
        "SELECT id, JSON_VALUE(s, '$.a' RETURNING INTEGER) FROM inputs");
  }
}
