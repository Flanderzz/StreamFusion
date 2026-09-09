package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkUrlDecodeSqlHarnessTest {
  @Test
  void formDecodingAndMalformedUtf8() throws Exception {
    parity("SELECT id, URL_DECODE(s), URL_DECODE(COALESCE(s, '')) FROM texts");
  }

  @Test
  void urlDecoderMatchesJavaAcrossByteAndUnicodeDigitBoundaries() throws Exception {
    NativeParity.assertParity(
        StringFunctionTestInputs::urlBoundaries, "SELECT id, URL_DECODE(s) FROM urls");
  }

  @Test
  void unverifiedOverloadsFallBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        StringFunctionTestInputs::text, "SELECT PARSE_URL(u, 'HOST') FROM texts", "PARSE_URL");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::text, sql);
  }
}
