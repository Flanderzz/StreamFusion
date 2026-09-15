package tech.streamfusion.planner;

import java.math.BigDecimal;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import tech.streamfusion.operator.WatermarkDelay;

/** Shared interval admission for standalone and pushed-down SQL watermarks. */
final class WatermarkInterval {
  private WatermarkInterval() {}

  static WatermarkDelay parse(RexNode node) {
    if (!(node instanceof RexLiteral)) {
      return null;
    }
    SqlTypeFamily family = node.getType().getSqlTypeName().getFamily();
    if (family != SqlTypeFamily.INTERVAL_DAY_TIME && family != SqlTypeFamily.INTERVAL_YEAR_MONTH) {
      return null;
    }
    BigDecimal value = ((RexLiteral) node).getValueAs(BigDecimal.class);
    if (value == null || value.signum() < 0) {
      return null;
    }
    try {
      switch (family) {
        case INTERVAL_DAY_TIME:
          return WatermarkDelay.millis(value.longValueExact());
        case INTERVAL_YEAR_MONTH:
          return WatermarkDelay.months(value.intValueExact());
        default:
          return null;
      }
    } catch (ArithmeticException outOfRange) {
      return null;
    }
  }
}
