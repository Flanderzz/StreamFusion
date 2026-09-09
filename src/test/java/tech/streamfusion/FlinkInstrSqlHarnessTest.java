package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkInstrSqlHarnessTest {
  @Test
  void positionsCountUnicodeCharacters() throws Exception {
    parity(
        "SELECT id, INSTR(s, needle), INSTR(s, ''), INSTR(s, 'abc'), INSTR('abc', needle), INSTR(s,"
            + " CAST(NULL AS STRING)) FROM searches");
  }

  @Test
  void predicatesAndNonNullableArguments() throws Exception {
    parity("SELECT id, INSTR(COALESCE(s, ''), 'b') FROM searches WHERE INSTR(s, 'b') > 0");
  }

  @Test
  void unverifiedOverloadsFallBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        StringFunctionTestInputs::search,
        "SELECT INSTR(s, needle, 2) FROM searches",
        "INSTR requires 2 arguments");
    NativeParity.assertFallbackReasonContains(
        StringFunctionTestInputs::search,
        "SELECT INSTR(s, needle, -1, 2) FROM searches",
        "INSTR requires 2 arguments");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::search, sql);
  }
}
