package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class JsonPathSpecTest {
  @Test
  void onlyDefiniteMemberAndSignedIndexPathsAreAdmitted() {
    for (String path :
        List.of(
            "$",
            "$.a.b[0]",
            "$['a b'][2147483647]",
            "$[01].a",
            "$[-1]",
            "$[-0]",
            "$[-0001].a",
            "$[-2147483648]",
            "$.\u7528\u6237['\u59d3.\u540d']",
            "$[\"O'Reilly\"]",
            "$['a\"b']",
            "$['']",
            "$[\"\"][''].a[0]",
            "$['\ud83d\ude00']")) {
      assertEquals("strict " + path, JsonPathSpec.normalize(path));
    }
    assertEquals("lax $.a", JsonPathSpec.normalize(" \tLaX $.a"));
    assertEquals("strict $.a", JsonPathSpec.normalize("$.a "));
    for (String path :
        List.of(
            "",
            "a",
            "$.*",
            "$..a",
            "$[-2147483649]",
            "$[--1]",
            "$[-]",
            "$[2147483648]",
            "$[]",
            "$[\"a\",\"b\"]",
            "$['\ud800']",
            "$['a\n']",
            "$[?(@.a)]")) {
      assertNull(JsonPathSpec.normalize(path), path);
    }
  }

  @Test
  void verifiedEscapesUseCanonicalJsonNamesWithoutLosingIdentity() {
    assertEquals("strict $[\"a\\\\b\"]", JsonPathSpec.normalize("$['a\\\\b']"));
    assertEquals("strict $[\"a'b\"]", JsonPathSpec.normalize("$['a\\'b']"));
    assertEquals("strict $[\"a\\\"b\"]", JsonPathSpec.normalize("$[\"a\\\"b\"]"));
    assertEquals("strict $[\"a\\bb\"]", JsonPathSpec.normalize("$['a\\bb']"));
    assertEquals("lax $[\"a/b\"][0]", JsonPathSpec.normalize("lax $[ 'a\\/b' ][ 0 ]"));
    assertEquals("strict $[\"用户\"]", JsonPathSpec.normalize("$['\\u7528\\u6237']"));
    assertEquals("strict $[\"😀\"]", JsonPathSpec.normalize("$['\\uD83D\\uDE00']"));
    for (String name :
        List.of("\\uD800", "\\uDC00", "\\uD800x\\uDC00", "\\u12", "\\uGGGG", "\\用户")) {
      assertNull(JsonPathSpec.normalize("$['" + name + "']"), name);
    }
    assertEquals("strict $[\"aq\"]", JsonPathSpec.normalize("$['a\\q']"));
    assertEquals("strict $[\"x61\"]", JsonPathSpec.normalize("$['\\x61']"));
    assertEquals("strict $[\"*\"]", JsonPathSpec.normalize("$['\\*']"));
  }

  @Test
  void onlyVerifiedPathSpacesAreNormalized() {
    assertEquals("strict $[0001]", JsonPathSpec.normalize("$[ 0001 ]"));
    assertEquals("lax $[' a b '][1]", JsonPathSpec.normalize(" \tLaX $[ ' a b ' ][ 1 ]  "));
    assertEquals("strict $[\"a'b\"]", JsonPathSpec.normalize("$[ \"a'b\" ]"));
    assertEquals("strict $[''][\"\"]", JsonPathSpec.normalize("$[ '' ][ \"\" ]"));
    for (String path :
        List.of(" $[1]", "$[\t1]", "$[1\n]", "$[\t'a']", "$[1 2]", "$.a\t", "$[1]\t", "$[1] \n")) {
      assertNull(JsonPathSpec.normalize(path), path);
    }
  }

  @Test
  void columnPathsFallBackBeforeNativeCompilation() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var text = types.createSqlType(SqlTypeName.VARCHAR);
    for (String name : List.of("JSON_VALUE", "JSON_EXISTS")) {
      var function =
          new SqlFunction(
              name, SqlKind.OTHER_FUNCTION, null, null, null, SqlFunctionCategory.SYSTEM);
      var output = name.equals("JSON_VALUE") ? text : types.createSqlType(SqlTypeName.BOOLEAN);
      var call =
          rex.makeCall(
              output, function, List.of(rex.makeInputRef(text, 0), rex.makeInputRef(text, 1)));
      var config = new org.apache.flink.configuration.Configuration();
      try (var ignored = NativeConfig.usePlannerConfig(config)) {
        assertNull(RexExpression.encodeProjections(List.of(call), List.of("v")));
        for (String path :
            List.of(
                "$.a",
                "$[-1]",
                "$[-2147483648]",
                "$[-0]",
                "$['a\\'b']",
                "$['a\\\\b']",
                "$['\\u0000']")) {
          var literalPath =
              rex.makeCall(
                  output, function, List.of(rex.makeInputRef(text, 0), rex.makeLiteral(path)));
          var encoded = RexExpression.encodeProjections(List.of(literalPath), List.of("v"));
          assertNotNull(encoded);
          var binding = encoded.udfBinding();
          long[] constants = encoded.longs();
          try {
            assertSame(
                constants, binding.bind(constants), "literal indexes must register no JVM UDF");
          } finally {
            binding.unbind();
          }
        }
      }
    }
  }
}
