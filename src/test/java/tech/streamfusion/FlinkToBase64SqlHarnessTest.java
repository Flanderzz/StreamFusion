package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkToBase64SqlHarnessTest {
  @Test
  void utf8AndPadding() throws Exception {
    parity("SELECT id, TO_BASE64(s) FROM encodings");
  }

  @Test
  void literalsNullsAndNonNullableArguments() throws Exception {
    parity(
        "SELECT id, TO_BASE64(CAST('ab' AS CHAR(4))), TO_BASE64(CAST(NULL AS STRING)),"
            + " TO_BASE64(COALESCE(s, '')) FROM encodings");
  }

  @Test
  void unverifiedOverloadsFallBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        StringFunctionTestInputs::encodings,
        "SELECT FROM_BASE64(CASE WHEN s IS NULL THEN CAST(NULL AS STRING) ELSE 'YQ==' END) FROM"
            + " encodings",
        "FROM_BASE64");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::encodings, sql);
  }

  @Test
  void binaryArgumentFallsBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        TextTimeFunctionTestInputs::bytes, "SELECT id, TO_BASE64(b) FROM inputs", "TO_BASE64");
  }
}
