package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.Arrays;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkCertifiedBuiltinSqlHarnessTest {
  @Test
  void regexCertificatesIncludeEmptyMatchesAndJavaLiteralReplacement() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::patterns,
        "SELECT REGEXP(s,p), REGEXP_REPLACE(s,p,r), REGEXP_EXTRACT(s,p,0), "
            + "REGEXP_EXTRACT(s,p,CAST(4294967296 AS BIGINT)) FROM src");
  }

  @Test
  void regexPositionsAndCountsPreserveJavaEmptyMatchProgression() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("REGEXP_COUNT");
    BuiltinFunctionParity.assertParity(
        this::patterns, "SELECT REGEXP_COUNT(s,p), REGEXP_INSTR(s,p), REGEXP_SUBSTR(s,p) FROM src");
  }

  @Test
  void likeEscapeAndSimilarPreserveNewlinesAndDynamicPatterns() throws Exception {
    BuiltinFunctionParity.assertParity(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("s", STRING()), FIELD("p", STRING()), FIELD("e", STRING())),
                Arrays.asList(
                    Row.of("a_b", "a!_b", "!"),
                    Row.of("a\nb", "a_b", "!"),
                    Row.of("a\rb", "a%b", "!"),
                    Row.of("abc", "a(b|d)c", "!"),
                    Row.of("ac", "a.c", "!"),
                    Row.of(null, "%", "!"))),
        "SELECT s LIKE p ESCAPE e, s NOT LIKE p ESCAPE e, s SIMILAR TO p, s NOT SIMILAR TO p FROM"
            + " src");
  }




  @Test
  void similarDoubledRegexEscapeRetainsFlinkTranslation() throws Exception {
    BuiltinFunctionParity.assertParity(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("s", STRING()), FIELD("p", STRING()), FIELD("e", STRING())),
                Arrays.asList(
                    Row.of("x", "..", "."),
                    Row.of(".", "..", "."),
                    Row.of("", "$$", "$"),
                    Row.of("$", "$$", "$"))),
        "SELECT s SIMILAR TO p ESCAPE e FROM src");
  }

  private TableEnvironment patterns() {
    return BuiltinFunctionParity.environment(
        ROW(FIELD("s", STRING()), FIELD("p", STRING()), FIELD("r", STRING())),
        Arrays.asList(
            Row.of("aa", "a*", "$1"),
            Row.of("a\rb", ".", "X"),
            Row.of("abc", "", "X"),
            Row.of("ab", "a(?=b)", "X"),
            Row.of("aab", "(a)\\1", "X"),
            Row.of("abc", "[", "X"),
            Row.of("\ud800", ".", "X"),
            Row.of("\ud800", "\\p{Cs}", "X"),
            Row.of(null, null, null)));
  }
}
