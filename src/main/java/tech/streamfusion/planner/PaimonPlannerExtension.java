package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;

/**
 * Paimon append-table sink rewrite contributed by streamfusion-paimon. Like every sink entry it
 * sits before the insert-only guard, because a root sink carries no changelog trait of its own; the
 * matcher checks the input's changelog mode itself and declines anything but an insert-only stream,
 * which is all stock Paimon accepts for an append table anyway.
 */
public final class PaimonPlannerExtension implements NativePlannerExtension {
  @Override
  public void addSubstitutions(List<Substitution<?>> substitutions) {
    substitutions.add(
        Substitution.of(StreamPhysicalSink.class, PaimonSinkMatcher::substitute)
            .matching(PaimonSinkMatcher::appliesTo)
            .changelogSafe());
  }
}
