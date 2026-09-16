package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkJsonPathGrammarSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "$[ 1 ]",
        "$[1 ]",
        "$[ 1]",
        "$[ 0001 ]",
        "$[ 'a' ]",
        "$[ \"a\" ]",
        "$['a' ]",
        "$[ 'a']",
        "$.a[ 1 ]",
        "$[ 'a b' ][ 1 ].q",
        "$[ 'a b' ][ 1 ][ 'q' ]",
        "$[1]  ",
        "$[ 2147483647 ]",
        "$.a ",
        "$[ ' a b ' ]"
      })
  void whitespacePathsMatchFlinkAcrossPolicies(String path) throws Exception {
    String sql = select(path);
    String plan = NativePlanner.explain(documents(), sql);
    assertTrue(plan.contains("NativeCalc"), plan);
    NativeParity.assertParity(FlinkJsonPathGrammarSqlHarnessTest::documents, sql);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "$[-1]",
        "$[+1]",
        "$[1,2]",
        "$[1:2]",
        "$[*]",
        "$..a",
        "$.a[*].b",
        "$[?(@.a)]",
        "$[2147483648]",
        "$[1 2]",
        "$[]",
        "$[\t1\t]",
        "$[ \n1 ]",
        "$[\t'a']",
        "$['a'\n]",
        " $[1] ",
        " $ ",
        "$[1]\t",
        "$[1]\n",
        "$.a\t",
        "$[1] \t",
        "$['a'] \n"
      })
  void unsupportedSelectorsRetainExplicitFallback(String path) throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkJsonPathGrammarSqlHarnessTest::documents,
        select(path),
        "literal definite member/index path");
  }

  @Test
  void multipleIndependentPathsShareNoSelectionState() throws Exception {
    String sql =
        "SELECT id, JSON_VALUE(s, '$[ 1 ]'), JSON_VALUE(s, '$[ 0 ]'), JSON_VALUE(s, '$[ ''a b'' ]["
            + " 1 ][ ''q'' ]'), JSON_EXISTS(s, '$[ ''a'' ]') FROM inputs";
    assertTrue(NativePlanner.explain(documents(), sql).contains("NativeCalc"));
    NativeParity.assertParity(FlinkJsonPathGrammarSqlHarnessTest::documents, sql);
  }

  @Test
  void whitespaceIndexPreservesIntegerReturningAndDefaults() throws Exception {
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                null, "[]", "[0,2147483647]", "[0,-2147483648]", "[0,null]", "invalid"),
        "SELECT id, JSON_VALUE(s, '$[ 1 ]' RETURNING INTEGER DEFAULT 7 ON EMPTY DEFAULT 9 ON ERROR)"
            + " FROM inputs");
  }

  @Test
  void whitespaceExistsPreservesHostException() {
    JsonFunctionTestInputs.assertFailsLikeFlink("[]", "JSON_EXISTS(s, '$[ 1 ]' ERROR ON ERROR)");
  }

  @ParameterizedTest
  @ValueSource(strings = {"$['a']", "$[ 'a' ]"})
  void valueErrorPolicyFailsForCompactAndSpacedPaths(String path) {
    // ERROR-policy failures retain their native wrapper; scalar type mismatches preserve the host exception.
    JsonFunctionTestInputs.assertFails(
        "{}",
        "JSON_VALUE(s, '" + path.replace("'", "''") + "' ERROR ON ERROR)",
        "JSON_VALUE ERROR");
  }

  @Test
  void invalidIntegerScalarFailsOutsideErrorPolicy() {
    var comparison = NativeFailureParity.run(
        () -> TextTimeFunctionTestInputs.textRows("[0,\"text\"]"),
        "SELECT JSON_VALUE(s, '$[ 1 ]' RETURNING INTEGER NULL ON ERROR) FROM inputs");
    comparison.assertFailure(ClassCastException.class,
        "java.lang.String cannot be cast to class java.lang.Integer",
        NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
    org.junit.jupiter.api.Assertions.assertEquals(comparison.host().rootCause().getMessage(),
        comparison.nativeRun().rootCause().getMessage());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "lax $[1]",
        "LaX $[1]",
        " lax $[1]",
        "\tlax $[1]",
        " \tLaX $[1]",
        "lax   $[ 1 ]",
        " \tLaX $[ 1 ]  ",
        "strict $[ 'a' ] "
      })
  void explicitModeWhitespaceMatchesFlink(String path) throws Exception {
    String quoted = path.replace("'", "''");
    String sql =
        "SELECT id, JSON_VALUE(s, '"
            + quoted
            + "'), "
            + "JSON_EXISTS(s, '"
            + quoted
            + "' UNKNOWN ON ERROR) FROM inputs";
    assertTrue(NativePlanner.explain(documents(), sql).contains("NativeCalc"));
    NativeParity.assertParity(FlinkJsonPathGrammarSqlHarnessTest::documents, sql);
  }

  private static String select(String path) {
    String quoted = path.replace("'", "''");
    return "SELECT id, JSON_EXISTS(s, '"
        + quoted
        + "' UNKNOWN ON ERROR), "
        + "JSON_EXISTS(s, 'lax "
        + quoted
        + "' UNKNOWN ON ERROR), "
        + "JSON_VALUE(s, '"
        + quoted
        + "' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), "
        + "JSON_VALUE(s, 'lax "
        + quoted
        + "' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR) FROM inputs";
  }

  private static TableEnvironment documents() {
    return TextTimeFunctionTestInputs.textRows(
        null,
        "",
        "null",
        "{}",
        "[]",
        "true",
        "42",
        "\"root\"",
        "[0,17]",
        "[0,null]",
        "[0,\"text\"]",
        "[0,[]]",
        "[0,{}]",
        "{\"a\":[0,17],\"a b\":[1,{\"q\":\"hello\"}]}",
        "{\"a\":\"value\",\"a b\":[1,{\"q\":null}]}",
        "{\"a\":false}",
        "{\"a\":null}",
        "{\"a\":\"old\",\"a\":\"new\"}",
        "{\" a b \":\"kept\",\"a b\":\"different\"}",
        "{\"a\":\"ok\",\"bad\":[}",
        "[0,17,tru]",
        "[0,17] trailing",
        "{\"a\":\"valid\",\"bad\":1e2147483648}");
  }
}
