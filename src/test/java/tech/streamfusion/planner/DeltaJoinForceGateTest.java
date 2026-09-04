package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

/**
 * Flink validates its FORCE delta-join strategy after our pass runs, and rejects a plan only when a
 * regular join survived. Substituting that join away would turn a query the host means to reject
 * into one that silently runs, so the pass declines such plans wholesale. The gate mirrors the
 * host's condition exactly rather than declining on FORCE alone — acceleration is kept for every
 * plan the validator would have passed.
 */
class DeltaJoinForceGateTest {

  private static final String JOIN_QUERY =
      "SELECT a.k, a.v, b.w FROM A AS a JOIN B AS b ON a.k = b.k";

  @Test
  void forceWithoutADeltaJoinLeavesThePlanForFlinkToReject() {
    TableEnvironment tEnv = environment();
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    tEnv.getConfig()
        .set(
            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
            OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

    ValidationException failure =
        assertThrows(ValidationException.class, () -> tEnv.explainSql(JOIN_QUERY));

    assertTrue(
        failure.getMessage().contains("delta join"),
        "expected Flink's own FORCE rejection, got: " + failure.getMessage());
    assertEquals(0, scan.substitutions(), scan::explainSummary);
    assertTrue(
        scan.fallbackReasons().stream().anyMatch(reason -> reason.startsWith("delta join:")),
        "the decline must be reported as a fallback reason, saw: " + scan.fallbackReasons());
  }

  @Test
  void forceStillAcceleratesAPlanWithoutAJoin() {
    TableEnvironment tEnv = environment();
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    tEnv.getConfig()
        .set(
            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
            OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

    tEnv.explainSql("SELECT k, v * 2 FROM A");

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
  }

  @Test
  void theDefaultStrategyAcceleratesTheSameJoin() {
    TableEnvironment tEnv = environment();
    PhysicalPlanScan scan = NativePlanner.install(tEnv);

    tEnv.explainSql(JOIN_QUERY);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
  }

  @Test
  void noneStrategyAcceleratesTheSameJoin() {
    TableEnvironment tEnv = environment();
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    tEnv.getConfig()
        .set(
            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
            OptimizerConfigOptions.DeltaJoinStrategy.NONE);

    tEnv.explainSql(JOIN_QUERY);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    DataStream<Row> a =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
            Row.of(1L, 10L),
            Row.of(2L, 20L));
    DataStream<Row> b =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "w"}, Types.LONG, Types.LONG),
            Row.of(1L, 100L),
            Row.of(2L, 200L));
    tEnv.createTemporaryView(
        "A",
        a,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.BIGINT()).build());
    tEnv.createTemporaryView(
        "B",
        b,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("w", DataTypes.BIGINT()).build());
    return tEnv;
  }
}
