package tech.streamfusion.planner;

import java.util.List;

final class FlinkPlannerCompat {
  private FlinkPlannerCompat() {}

  static org.apache.calcite.rel.RelNode prepareForRewrite(org.apache.calcite.rel.RelNode node) {
    return node;
  }

  static void addSubstitutions(List<Substitution<?>> entries) {}
}
