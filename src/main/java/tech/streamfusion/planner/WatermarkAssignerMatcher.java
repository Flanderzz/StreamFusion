package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import tech.streamfusion.operator.WatermarkDelay;

/**
 * Recognizes a {@link StreamPhysicalWatermarkAssigner} the native columnar assigner can reproduce:
 * the bounded out-of-orderness form, where the watermark is the rowtime itself ({@code WATERMARK
 * FOR rt AS rt}, delay 0) or the rowtime minus a day-time or year-month interval constant ({@code
 * rt - INTERVAL '5' SECOND}). Any other watermark expression — a non-constant delay, a different
 * column, an unsupported function — falls back to the host. The interval unit is preserved through
 * execution.
 */
final class WatermarkAssignerMatcher {

  private WatermarkAssignerMatcher() {}

  static boolean matches(StreamPhysicalWatermarkAssigner wm) {
    return delay(wm) != null;
  }

  static int rowtimeColumn(StreamPhysicalWatermarkAssigner wm) {
    return wm.rowtimeFieldIndex();
  }

  /** The constant delay with its interval unit, or null if the expression is not that shape. */
  static WatermarkDelay delay(StreamPhysicalWatermarkAssigner wm) {
    RexNode expr = wm.watermarkExpr();
    int rowtime = wm.rowtimeFieldIndex();
    // WATERMARK FOR rt AS rt — no delay.
    if (expr instanceof RexInputRef && ((RexInputRef) expr).getIndex() == rowtime) {
      return WatermarkDelay.millis(0);
    }
    // WATERMARK FOR rt AS rt - INTERVAL <constant>.
    if (expr instanceof RexCall) {
      RexCall call = (RexCall) expr;
      if (call.getOperator().getKind() == SqlKind.MINUS && call.getOperands().size() == 2) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        if (left instanceof RexInputRef && ((RexInputRef) left).getIndex() == rowtime) {
          return WatermarkInterval.parse(right);
        }
      }
    }
    return null;
  }

  static RelNode substitute(StreamPhysicalWatermarkAssigner wm, PlanContext ctx) {
    return new StreamPhysicalNativeWatermarkAssigner(
        wm.getCluster(),
        wm.getTraitSet(),
        wm.getInputs().get(0),
        wm.getRowType(),
        WatermarkAssignerMatcher.rowtimeColumn(wm),
        WatermarkAssignerMatcher.delay(wm));
  }
}
