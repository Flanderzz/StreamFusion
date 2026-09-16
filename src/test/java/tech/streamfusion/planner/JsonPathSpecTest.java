package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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
  void onlyDefiniteMemberAndNonnegativeIndexPathsAreAdmitted() {
    for (String path :
        List.of(
            "$",
            "$.a.b[0]",
            "$['a b'][2147483647]",
            "$[01].a",
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
            "$[-1]",
            "$[2147483648]",
            "$[]",
            "$['a\\b']",
            "$[\"a\",\"b\"]",
            "$['\ud800']",
            "$['a\n']",
            "$[?(@.a)]")) {
      assertNull(JsonPathSpec.normalize(path), path);
    }
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
      var literalPath =
          rex.makeCall(
              output, function, List.of(rex.makeInputRef(text, 0), rex.makeLiteral("$.a")));
      var config = new org.apache.flink.configuration.Configuration();
      try (var ignored = NativeConfig.usePlannerConfig(config)) {
        assertNull(RexExpression.encodeProjections(List.of(call), List.of("v")));
        assertNotNull(RexExpression.encodeProjections(List.of(literalPath), List.of("v")));
      }
    }
  }
}
