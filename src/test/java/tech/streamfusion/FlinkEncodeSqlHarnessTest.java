package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkEncodeSqlHarnessTest {
  @Test
  void encodesUnicodeNullsAndUnmappableCharacters() throws Exception {
    NativeParity.assertParity(
        StringFunctionTestInputs::encodings,
        "SELECT id, ENCODE(s, 'UTF-8'), ENCODE(s, 'ASCII'), ENCODE(s, 'latin1') FROM encodings");
  }

  @Test
  void unverifiedFormsFallBackBeforeExecution() throws Exception {
    NativeParity.assertFallback(
        StringFunctionTestInputs::encodings, "SELECT id, ENCODE(s, 'UTF-16') FROM encodings");
  }

  @Test
  void dynamicCharsetFallsBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, ENCODE(s, CASE WHEN n > 0 THEN 'UTF-8' ELSE 'ASCII' END) FROM inputs",
        "literal charset");
  }
}
