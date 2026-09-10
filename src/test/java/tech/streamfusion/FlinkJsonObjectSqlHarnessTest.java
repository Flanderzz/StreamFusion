package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkJsonObjectSqlHarnessTest {
  @Test
  void scalarValuesNullPoliciesAndNonNullableInputs() throws Exception {
    NativeParity.assertParity(
        StringFunctionTestInputs::encodings,
        "SELECT id, JSON_OBJECT('z' VALUE s, 'b' VALUE n > 0, 'a' VALUE n),"
            + " JSON_OBJECT('s' VALUE s, 'n' VALUE n ABSENT ON NULL),"
            + " JSON_OBJECT('s' VALUE COALESCE(s, ''), 'n' VALUE COALESCE(n, 0)),"
            + " JSON_OBJECT() FROM encodings");
  }

  @Test
  void duplicatesKeepLastInsertedValueUnderEachNullPolicy() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::strings,
        "SELECT id, JSON_OBJECT('k' VALUE id, 'k' VALUE s),"
            + " JSON_OBJECT('k' VALUE id, 'k' VALUE s ABSENT ON NULL),"
            + " JSON_OBJECT('k' VALUE s, 'k' VALUE CAST(NULL AS STRING)),"
            + " JSON_OBJECT('k' VALUE s, 'k' VALUE CAST(NULL AS STRING) ABSENT ON NULL)"
            + " FROM inputs");
  }

  @Test
  void keysUseUtf16OrderAndValuesUseJacksonEscaping() throws Exception {
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                null,
                "",
                "\u0000\u001f\t/\\\"",
                "\ud83d\ude00\u4e2d",
                "\u007f\u0080",
                "\u4e2d\ud83d\ude00".repeat(2048)),
        "SELECT id, JSON_OBJECT('\ufffd' VALUE id, '\ud83d\ude00' VALUE s,"
            + " '' VALUE s, 'a\"b\\c' VALUE s, '\u001f' VALUE s) FROM inputs");
  }

  @Test
  void allIntegerWidthsPredicatesAndCaseExpressions() throws Exception {
    NativeParity.assertParity(
        StringFunctionTestInputs::encodings,
        "SELECT id, JSON_OBJECT('tiny' VALUE CAST(n AS TINYINT),"
            + " 'small' VALUE CAST(n AS SMALLINT), 'int' VALUE CAST(n AS INT),"
            + " 'long' VALUE n, 'value' VALUE CASE WHEN n > 0 THEN s ELSE '' END)"
            + " FROM encodings WHERE JSON_OBJECT('s' VALUE s ABSENT ON NULL) <> '{}'");
  }

  @Test
  void constructorInsideCaseIsAnOrdinaryStringValue() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::strings,
        "SELECT id, JSON_OBJECT('v' VALUE CASE WHEN s IS NULL THEN '{}'"
            + " ELSE JSON_OBJECT('s' VALUE s) END) FROM inputs");
  }

  @Test
  void unverifiedValuesAndNestedRawJsonFallBack() throws Exception {
    for (String expression :
        new String[] {
          "CAST(n AS DOUBLE)",
          "CAST(n AS DECIMAL(20,2))",
          "ARRAY[n]",
          "JSON_OBJECT('n' VALUE n)",
          "JSON_ARRAY(n)",
          "JSON(CASE WHEN n > 0 THEN '{}' ELSE '[]' END)"
        }) {
      NativeParity.assertFallbackReasonContains(
          StringFunctionTestInputs::encodings,
          "SELECT id, JSON_OBJECT('value' VALUE " + expression + ") FROM encodings",
          "JSON_OBJECT");
    }
  }
}
