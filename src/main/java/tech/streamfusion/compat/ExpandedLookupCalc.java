package tech.streamfusion.compat;

import java.util.List;
import org.apache.calcite.rex.RexNode;

public record ExpandedLookupCalc(List<RexNode> projection, RexNode filter) {}
