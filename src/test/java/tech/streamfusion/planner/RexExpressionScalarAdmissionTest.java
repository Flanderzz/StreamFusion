package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class RexExpressionScalarAdmissionTest {
  @Test
  void jsonObjectDeclinesDynamicNullAndMalformedUnicodeKeys() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var string = types.createSqlType(SqlTypeName.VARCHAR);
    var object =
        new SqlFunction(
            "JSON_OBJECT", SqlKind.OTHER_FUNCTION, null, null, null, SqlFunctionCategory.STRING);
    assertThrows(CalciteException.class, () -> rex.makeLiteral("\ud800"));
    for (var key : List.of(rex.makeInputRef(string, 0), rex.makeNullLiteral(string))) {
      var call =
          rex.makeCall(
              string,
              object,
              List.of(
                  rex.makeFlag(SqlJsonConstructorNullClause.NULL_ON_NULL),
                  key,
                  rex.makeInputRef(types.createSqlType(SqlTypeName.INTEGER), 1)));
      assertNull(RexExpression.encodeProjections(List.of(call), List.of("object")));
    }
  }

  @Test
  void jsonStringRequiresTheScalarTypeThatFlinkCanSerialize() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var function =
        new SqlFunction(
            "JSON_STRING", SqlKind.OTHER_FUNCTION, null, null, null, SqlFunctionCategory.STRING);
    var call =
        rex.makeCall(
            types.createSqlType(SqlTypeName.VARCHAR),
            function,
            List.of(rex.makeNullLiteral(types.createSqlType(SqlTypeName.NULL))));
    assertNull(RexExpression.encodeProjections(List.of(call), List.of("json")));
  }

  @Test
  void hexRejectsBooleanEvenIfAnUpstreamPlannerSuppliesTheCall() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var hex =
        new SqlFunction(
            "HEX", SqlKind.OTHER_FUNCTION, null, null, null, SqlFunctionCategory.STRING);
    var call =
        rex.makeCall(
            types.createSqlType(SqlTypeName.VARCHAR),
            hex,
            List.of(rex.makeInputRef(types.createSqlType(SqlTypeName.BOOLEAN), 0)));
    assertNull(RexExpression.encodeProjections(List.of(call), List.of("hex")));
  }
}
