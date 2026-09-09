package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkSplitSqlHarnessTest {
  @Test
  void splitsLiteralSeparatorsAndPreservesEmptyTokens() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::strings,
        "SELECT id, SPLIT(s, '|'), SPLIT(s, '.*'), SPLIT(s, '\u4e2d') FROM inputs");
  }

  @Test
  void unverifiedFormsFallBackBeforeExecution() throws Exception {
    NativeParity.assertFallback(
        TextTimeFunctionTestInputs::strings, "SELECT id, SPLIT(s, '') FROM inputs");
  }
}
