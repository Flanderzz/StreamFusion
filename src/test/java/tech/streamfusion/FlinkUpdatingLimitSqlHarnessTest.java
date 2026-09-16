package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkUpdatingLimitSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(ints = {1, 2, 100})
  void monotonicCountsUseUpdateFastRank(int limit) throws Exception {
    assertNative(() -> environment(false, false),
        "SELECT k, COUNT(*) AS n FROM src GROUP BY k ORDER BY n DESC, k ASC NULLS FIRST LIMIT " + limit,
        true, "UpdateFastStrategy");
  }

  @ParameterizedTest
  @ValueSource(strings = {"LIMIT 2", "LIMIT 100"})
  void sumsWithDecrementsAndGroupDeletionUseRetractRank(String range) throws Exception {
    assertNative(() -> environment(true, false),
        "SELECT k, SUM(v) AS total FROM src GROUP BY k "
            + "ORDER BY total DESC NULLS LAST, k ASC NULLS FIRST " + range,
        true, "RetractStrategy");
  }

  @ParameterizedTest
  @ValueSource(strings = {"LIMIT 2", "LIMIT 100"})
  void unorderedUpdatingLimitMatchesHost(String range) throws Exception {
    assertNative(() -> environment(true, false),
        "SELECT k, SUM(v) AS total FROM src GROUP BY k " + range, true, null);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void miniBatchPreservesMaterializedSelection(boolean changes) throws Exception {
    assertNative(() -> environment(changes, true),
        "SELECT k, SUM(v) AS total FROM src GROUP BY k ORDER BY total DESC NULLS LAST, k ASC LIMIT 2",
        false, null);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 100})
  void updateFastOffsetMatchesHost(int offset) throws Exception {
    String sql = "SELECT k, COUNT(*) AS n FROM src GROUP BY k "
        + "ORDER BY n DESC, k ASC NULLS FIRST LIMIT 2 OFFSET " + offset;
    assertNative(() -> environment(false, false), sql, true, "UpdateFastStrategy");
    NativeParity.assertChangelogParity(() -> environment(false, false), sql);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "ORDER BY total DESC NULLS LAST, k ASC NULLS FIRST "})
  void retractingOffsetPreservesHostStoredRowKinds(String order) throws Exception {
    String sql = "SELECT k, SUM(v) AS total FROM src GROUP BY k " + order + "LIMIT 2 OFFSET 1";
    NativeParity.assertFallbackReasonContains(() -> environment(true, false), sql,
        "retracting OFFSET without projected rank requires Flink's stored-row-kind semantics");
  }

  @Test
  void nullableDuplicatesWithOffsetMatchHost() throws Exception {
    Supplier<TableEnvironment> input = () -> {
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      var table = StreamTableEnvironment.create(env);
      table.createTemporaryView("src", table.fromChangelogStream(env.fromData(
          Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
          Row.of(1L, 20L), Row.of(1L, 20L), Row.of(2L, 10L), Row.of(null, null),
          Row.ofKind(RowKind.DELETE, 1L, 20L), Row.of(3L, 5L),
          Row.ofKind(RowKind.DELETE, 2L, 10L))));
      return table;
    };
    String sql = "SELECT k, v FROM src ORDER BY v DESC NULLS LAST, k ASC LIMIT 2 OFFSET 1";
    NativeParity.assertFallbackReasonContains(input, sql,
        "retracting OFFSET without projected rank requires Flink's stored-row-kind semantics");
    String ranked = "SELECT k, v, rn FROM (SELECT k, v, ROW_NUMBER() OVER "
        + "(ORDER BY v DESC NULLS LAST, k ASC) AS rn FROM src) WHERE rn BETWEEN 2 AND 3";
    assertTrue(NativePlanner.explain(input.get(), ranked).contains("NativeColumnarTopN"));
    NativeParity.assertOrderedKindedParity(input, ranked);
    NativeParity.assertChangelogParity(input, ranked);
    NativeParity.assertFallbackReasonContains(input, ranked.replace("SELECT k, v, rn FROM", "SELECT k, v FROM"),
        "retracting OFFSET without projected rank requires Flink's stored-row-kind semantics");
  }

  private static void assertNative(Supplier<TableEnvironment> environment, String sql,
      boolean kinded, String strategy) throws Exception {
    if (strategy != null) {
      assertTrue(environment.get().explainSql(sql).contains(strategy), "expected " + strategy);
    }
    String plan = NativePlanner.explain(environment.get(), sql);
    assertTrue(plan.contains("NativeColumnarTopN"), plan);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    if (kinded && sql.contains("OFFSET")) NativeParity.assertOrderedKindedParity(environment, sql);
    else if (kinded) NativeParity.assertKindedParity(environment, sql);
    else NativeParity.assertChangelogParity(environment, sql);
  }

  private static TableEnvironment environment(boolean changes, boolean miniBatch) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    if (miniBatch) {
      table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
      table.getConfig().set("table.exec.mini-batch.enabled", "true");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
      table.getConfig().set("table.exec.mini-batch.size", "4");
    }
    var type = Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG);
    Row[] rows = {Row.of(null, null), Row.of(1L, 5L), Row.of(2L, 3L), Row.of(3L, 1L),
        Row.of(2L, 9L), Row.of(3L, 8L), Row.of(3L, 6L), Row.of(1L, -4L),
        Row.of(2L, 2L), Row.of(2L, 1L), Row.of(null, null)};
    if (changes) {
      Row[] updates = java.util.Arrays.copyOf(rows, rows.length + 6);
      updates[rows.length] = Row.ofKind(RowKind.DELETE, 3L, 8L);
      updates[rows.length + 1] = Row.ofKind(RowKind.DELETE, 3L, 6L);
      updates[rows.length + 2] = Row.ofKind(RowKind.DELETE, 3L, 1L);
      updates[rows.length + 3] = Row.ofKind(RowKind.UPDATE_BEFORE, 2L, 9L);
      updates[rows.length + 4] = Row.ofKind(RowKind.UPDATE_AFTER, 2L, -9L);
      updates[rows.length + 5] = Row.of(4L, 20L);
      table.createTemporaryView("src", table.fromChangelogStream(env.fromData(type, updates)));
    } else {
      table.createTemporaryView("src", env.fromData(type, rows));
    }
    return table;
  }
}
