package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate;

/** Admission for released Flink JVM table-function generation inside a columnar boundary. */
final class TableFunctionMatcher {
  private TableFunctionMatcher() {}

  static boolean isUserTableFunction(StreamPhysicalCorrelate node) {
    if (!(node.scan().getCall() instanceof RexCall)) return false;
    var operator = ((RexCall) node.scan().getCall()).getOperator();
    return operator instanceof BridgingSqlFunction
        && ((BridgingSqlFunction) operator).getDefinition() instanceof TableFunction;
  }

  static boolean matches(StreamPhysicalCorrelate node) {
    var function =
        ((BridgingSqlFunction) ((RexCall) node.scan().getCall()).getOperator()).getDefinition();
    if (function instanceof org.apache.flink.table.functions.SpecializedFunction) return false;
    JoinRelType join = UnnestMatcher.joinType(node);
    return (join == JoinRelType.INNER || join == JoinRelType.LEFT)
        && basicRow(node.getInput().getRowType())
        && basicRow(node.getRowType())
        && ((RexCall) node.scan().getCall())
            .getOperands().stream().allMatch(arg -> basicType(arg.getType()));
  }

  private static boolean basicRow(RelDataType type) {
    return type.getFieldList().stream().allMatch(field -> basicType(field.getType()));
  }

  private static boolean basicType(RelDataType type) {
    if (type.getSqlTypeName().getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME
        || type.getSqlTypeName().getFamily() == SqlTypeFamily.INTERVAL_YEAR_MONTH) return true;
    switch (type.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
      case FLOAT:
      case DOUBLE:
      case VARBINARY:
      case INTEGER:
      case BIGINT:
        return true;
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case DECIMAL:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return true;
      default:
        return false;
    }
  }

  static String unsupportedReason(StreamPhysicalCorrelate node) {
    return "correlate: only non-specialized JVM table functions with INNER/LEFT and CHAR/VARCHAR,"
               + " numeric, boolean, VARBINARY, interval or temporal input, output and argument"
               + " types are verified";
  }

  static RelNode substitute(StreamPhysicalCorrelate node, PlanContext context) {
    return new StreamPhysicalNativeTableFunction(
        node.getCluster(),
        node.getTraitSet(),
        node.getInput(),
        node.getRowType(),
        (RexCall) node.scan().getCall(),
        node.condition().isDefined() ? node.condition().get() : null,
        UnnestMatcher.joinType(node) == JoinRelType.LEFT);
  }
}
