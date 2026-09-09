package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkBtrimSqlHarnessTest {
  @Test
  void btrimUsesCharacterSetsAndPreservesNonSpaceWhitespace() throws Exception {
    parity(
        "SELECT id, BTRIM(s), BTRIM(s, ' ab'), BTRIM(s, ''),"
            + " BTRIM(s, CAST(NULL AS STRING)) FROM texts");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::text, sql);
  }

  @Test
  void dynamicTrimSetsFallBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, BTRIM(s, p) FROM inputs",
        "literal trim set");
  }
}
