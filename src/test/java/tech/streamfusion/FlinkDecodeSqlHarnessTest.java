package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkDecodeSqlHarnessTest {
  @Test
  void decodesBytesAndReplacesMalformedSequencesLikeTheJdk() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::bytes,
        "SELECT id, DECODE(b, 'UTF-8'), DECODE(b, 'ASCII'), DECODE(b, 'latin1') FROM inputs");
  }

  @Test
  void unverifiedFormsFallBackBeforeExecution() throws Exception {
    NativeParity.assertFallback(
        TextTimeFunctionTestInputs::bytes, "SELECT id, DECODE(b, 'UTF-16') FROM inputs");
  }

  @Test
  void validUtf8OnlyBatchUsesTheBufferReusePath() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::strings,
        "SELECT id, DECODE(ENCODE(s, 'UTF-8'), 'UTF-8') FROM inputs");
  }

  @Test
  void dynamicCharsetFallsBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, DECODE(ENCODE(s, 'UTF-8'), CASE WHEN n > 0 THEN 'UTF-8' ELSE 'ASCII' END) FROM"
            + " inputs",
        "literal charset");
  }
}
