package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class IfAdmissionTest {
  @Test
  void unregisteredHostOverloadsStayOutsideNativeCaseLowering() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var condition = rex.makeInputRef(types.createSqlType(SqlTypeName.BOOLEAN), 0);
    for (var type :
        List.of(
            types.createSqlType(SqlTypeName.BOOLEAN),
            types.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3),
            types.createArrayType(types.createSqlType(SqlTypeName.INTEGER), -1))) {
      var branch = rex.makeInputRef(type, 1);
      var call = rex.makeCall(type, FlinkSqlOperatorTable.IF, List.of(condition, branch, branch));
      assertNull(RexExpression.encodeProjections(List.of(call), List.of("v")), type.toString());
    }
  }
}
