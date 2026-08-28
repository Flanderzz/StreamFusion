package tech.streamfusion.planner.compat;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rex.RexNode;

/**
 * A dimension-side calc split into its projection and optional filter.
 *
 * <p>Flink returns the projection as a Scala {@code Seq} on 2.1 and a {@code java.util.List} on 2.2.
 * The two erase to the same descriptor, so the difference is invisible to bytecode comparison and
 * only shows up when compiling — hence this holder rather than the raw tuple.
 */
public final class ExpandedCalc {

  private final List<RexNode> projection;
  private final @Nullable RexNode filter;

  public ExpandedCalc(List<RexNode> projection, @Nullable RexNode filter) {
    this.projection = projection;
    this.filter = filter;
  }

  public List<RexNode> projection() {
    return projection;
  }

  public @Nullable RexNode filter() {
    return filter;
  }
}
