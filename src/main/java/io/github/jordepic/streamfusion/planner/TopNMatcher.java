package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;

/**
 * Recognizes the streaming Top-N the native ranker implements:
 * {@code ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) BETWEEN rankStart AND rankEnd}, with or
 * without the rank number projected. Requires {@code ROW_NUMBER} (Flink rejects streaming
 * RANK/DENSE_RANK), a constant rank range, and input/output column types the row/Arrow conversion
 * supports. The caller picks the ranker: the append-only one for an insert-only, no-offset query, or
 * the retracting one (full buffer, rank window {@code [offset+1, rankEnd]}) for a changelog input or
 * an {@code OFFSET} (rank start > 1).
 */
final class TopNMatcher {

  private TopNMatcher() {}

  static boolean matches(StreamPhysicalRank rank) {
    return unsupportedReason(rank) == null;
  }

  /** The specific reason this rank is not accelerable, or null if it is. */
  static String unsupportedReason(StreamPhysicalRank rank) {
    // With a TTL set the host expires rank state per entry (the buffer thins out silently) —
    // semantics the native rankers do not yet reproduce.
    if (IdleStateRetention.isEnabled(rank)) {
      return "Top-N: idle-state TTL (table.exec.state.ttl) runs on the host until the native"
          + " operator expires state";
    }
    if (rank.rankType() != RankType.ROW_NUMBER) {
      return "Top-N: only ROW_NUMBER ranks (RANK/DENSE_RANK fall back)";
    }
    if (!(rank.rankRange() instanceof ConstantRankRange)) {
      return "Top-N: only a constant rank range";
    }
    if (DeduplicateMatcher.isTimeOrder(rank)) {
      // A time-ordered rank is deduplication (DeduplicateMatcher), not a value Top-N.
      return "Top-N: a time-ordered rank is deduplication, not a value Top-N";
    }
    // The whole row crosses the boundary unchanged, so every column (incl. partition/order keys)
    // must be a type the conversion handles.
    if (!RowDataArrowConverter.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(rank.getRowType()))) {
      return "Top-N: a column type the boundary cannot carry";
    }
    return null;
  }

  static int[] partitionColumns(StreamPhysicalRank rank) {
    return rank.partitionKey().toArray();
  }

  /** The rank window upper bound (rankEnd): the operator emits ranks {@code [offset+1, limit]}. */
  static long limit(StreamPhysicalRank rank) {
    return ((ConstantRankRange) rank.rankRange()).getRankEnd();
  }

  /** The 0-based offset (rankStart - 1); > 0 for an {@code OFFSET} (range not starting at rank 1). */
  static long offset(StreamPhysicalRank rank) {
    return ((ConstantRankRange) rank.rankRange()).getRankStart() - 1;
  }

  static boolean outputRankNumber(StreamPhysicalRank rank) {
    return rank.outputRankNumber();
  }

  static int[] sortIndices(StreamPhysicalRank rank) {
    return rank.orderKey().getFieldCollations().stream()
        .mapToInt(RelFieldCollation::getFieldIndex)
        .toArray();
  }

  static int[] sortAscending(StreamPhysicalRank rank) {
    return rank.orderKey().getFieldCollations().stream()
        .mapToInt(fc -> fc.getDirection().isDescending() ? 0 : 1)
        .toArray();
  }

  static int[] sortNullsFirst(StreamPhysicalRank rank) {
    return rank.orderKey().getFieldCollations().stream()
        .mapToInt(fc -> nullsFirst(fc) ? 1 : 0)
        .toArray();
  }

  /** Whether nulls sort first for this column, resolving the unspecified case from the direction. */
  private static boolean nullsFirst(RelFieldCollation fc) {
    RelFieldCollation.NullDirection effective =
        fc.nullDirection == RelFieldCollation.NullDirection.UNSPECIFIED
            ? fc.getDirection().defaultNullDirection()
            : fc.nullDirection;
    return effective == RelFieldCollation.NullDirection.FIRST;
  }
}
