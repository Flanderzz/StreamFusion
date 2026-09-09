package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkStartsWithSqlHarnessTest {
  @Test
  void prefixArgumentsAndWildcards() throws Exception {
    parity(
        "SELECT id, STARTSWITH(s, needle), STARTSWITH(s, ''), STARTSWITH(s, 'ab'),"
            + " STARTSWITH('abc', needle), STARTSWITH(s, '%_'), STARTSWITH(s, '\\'), STARTSWITH(s,"
            + " CAST(NULL AS STRING)) FROM searches");
  }

  @Test
  void predicatesAndNonNullableArguments() throws Exception {
    parity(
        "SELECT id, STARTSWITH(COALESCE(s, ''), 'ab'), STARTSWITH(CAST('abc' AS CHAR(3)), 'a') FROM"
            + " searches WHERE STARTSWITH(s, 'ab')");
  }

  @Test
  void unverifiedOverloadsFallBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        StringFunctionTestInputs::search,
        "SELECT STARTSWITH(binary_value, X'FF') FROM searches",
        "STARTSWITH requires");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::search, sql);
  }
}
