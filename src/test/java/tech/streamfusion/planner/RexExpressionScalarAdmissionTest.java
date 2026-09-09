package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class RexExpressionScalarAdmissionTest {
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
