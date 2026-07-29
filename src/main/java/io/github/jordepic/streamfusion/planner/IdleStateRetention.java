package io.github.jordepic.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/** The job-wide idle-state retention ({@code table.exec.state.ttl}) as seen from a physical node. */
final class IdleStateRetention {

  private IdleStateRetention() {}

  /** Whether state TTL is enabled for the job (Flink treats a zero retention as "never expire"). */
  static boolean isEnabled(RelNode node) {
    return !ShortcutUtils.unwrapTableConfig(node)
        .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
        .isZero();
  }
}
