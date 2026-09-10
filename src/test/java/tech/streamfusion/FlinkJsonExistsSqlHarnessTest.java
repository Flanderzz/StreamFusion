package tech.streamfusion;

import static tech.streamfusion.NativeParity.assertFallback;
import static tech.streamfusion.NativeParity.assertParity;

import org.junit.jupiter.api.Test;

class FlinkJsonExistsSqlHarnessTest {
  @Test
  void nativeAdmissionDoesNotRequireCompatibilityOptIn() throws Exception {
    NativeParity.assertParity(
        JsonFunctionTestInputs::documents, "SELECT id, JSON_EXISTS(s, '$.a') FROM inputs");
  }

  @Test
  void strictLaxAndAllNonThrowingErrorPolicies() throws Exception {
    assertParity(
        JsonFunctionTestInputs::documents,
        "SELECT id, JSON_EXISTS(s, '$.a'), JSON_EXISTS(s, '$.a' TRUE ON ERROR), "
            + "JSON_EXISTS(s, '$.a' UNKNOWN ON ERROR), JSON_EXISTS(s, 'lax $.a'), "
            + "JSON_EXISTS(s, 'lax $.a' TRUE ON ERROR), "
            + "JSON_EXISTS(s, 'lax $.a' UNKNOWN ON ERROR) FROM inputs");
  }

  @Test
  void containersRootAndNestedPaths() throws Exception {
    assertParity(
        JsonFunctionTestInputs::documents,
        "SELECT id, JSON_EXISTS(s, '$'), JSON_EXISTS(s, 'lax $'), "
            + "JSON_EXISTS(s, '$.a.b[1]' UNKNOWN ON ERROR), JSON_EXISTS(s, '$[1].a'), "
            + "JSON_EXISTS(s, '$[''a b'']'), JSON_EXISTS(s, ' StRiCt $.a') FROM inputs");
  }

  @Test
  void wideDocumentsKeepDuplicateKeysAndUnselectedFieldValidation() throws Exception {
    assertParity(
        JsonFunctionTestInputs::wideDocuments,
        "SELECT id, JSON_EXISTS(s, '$.a' UNKNOWN ON ERROR), "
            + "JSON_EXISTS(s, 'lax $.a'), JSON_EXISTS(s, '$.a[1].b'), "
            + "JSON_EXISTS(s, '$.field63'), JSON_EXISTS(s, '$.absent' TRUE ON ERROR) FROM inputs");
  }

  @Test
  void unknownBooleanContextsKeepFlinkBoxedNullFailure() {
    JsonFunctionTestInputs.assertFallbackFails(
        "null", "JSON_EXISTS(s, '$' UNKNOWN ON ERROR) IS TRUE", "NullPointerException");
  }

  @Test
  void errorPolicyKeepsFlinkRowShortCircuiting() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> TextTimeFunctionTestInputs.textRows("{\"a\":1}", "invalid"),
        "SELECT id, id = 1 OR JSON_EXISTS(s, '$.a' ERROR ON ERROR) FROM inputs",
        "row short-circuiting");
  }

  @Test
  void nativePredicateKeepsTheMatchingRows() throws Exception {
    assertParity(
        JsonFunctionTestInputs::documents, "SELECT id FROM inputs WHERE JSON_EXISTS(s, 'lax $.a')");
  }

  @Test
  void errorPolicyFailsTheJob() {
    JsonFunctionTestInputs.assertFails(
        "{}", "JSON_EXISTS(s, '$.a' ERROR ON ERROR)", "JSON_EXISTS ERROR");
    JsonFunctionTestInputs.assertFails(
        "null", "JSON_EXISTS(s, 'lax $' ERROR ON ERROR)", "JSON_EXISTS ERROR");
  }

  @Test
  void wildcardAndRecursivePathsFallBack() throws Exception {
    assertFallback(
        JsonFunctionTestInputs::documents, "SELECT id, JSON_EXISTS(s, '$.*') FROM inputs");
    assertFallback(
        JsonFunctionTestInputs::documents, "SELECT id, JSON_EXISTS(s, '$..a') FROM inputs");
  }

  @Test
  void constraintsAndInvalidUnselectedValues() throws Exception {
    assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                "[".repeat(1000) + "0" + "]".repeat(1000),
                "[".repeat(1001) + "0" + "]".repeat(1001),
                "{\"a\":1,\"" + "k".repeat(50_001) + "\":2}",
                "{\"a\":1,\"bad\":1e2147483648}",
                "{\"a\":1,\"bad\":\"\\ud800\"}"),
        "SELECT id, JSON_EXISTS(s, '$' UNKNOWN ON ERROR), "
            + "JSON_EXISTS(s, '$.a' UNKNOWN ON ERROR), JSON_EXISTS(s, 'lax $.a') FROM inputs");
  }
}
