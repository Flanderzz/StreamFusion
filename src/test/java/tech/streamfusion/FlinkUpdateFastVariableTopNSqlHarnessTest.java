package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkUpdateFastVariableTopNSqlHarnessTest {
  static Stream<Arguments> queries() {
    return Stream.of(
            "k", "MOD(k, 3) + 1", "CAST(MOD(k, 3) + 1 AS INT)", "CAST(MOD(k, 3) + 1 AS SMALLINT)")
        .flatMap(
            bound ->
                Stream.of(false, true)
                    .flatMap(
                        rank ->
                            Stream.of(false, true).map(ties -> Arguments.of(bound, rank, ties))));
  }

  @ParameterizedTest
  @MethodSource("queries")
  void updateFastBoundsMatchReleasedFlink(String bound, boolean rank, boolean ties)
      throws Exception {
    String sql = query(bound, rank, ties);
    String hostPlan = environment(false).explainSql(sql);
    assertTrue(hostPlan.contains("UpdateFastStrategy"), hostPlan);
    String plan = NativePlanner.explain(environment(false), sql);
    assertTrue(plan.contains("NativeColumnarTopN"), plan);
    NativeParity.assertOrderedKindedParity(() -> environment(false), sql);
    NativeParity.assertChangelogParity(() -> environment(false), sql);
  }

  @ParameterizedTest
  @MethodSource("computedPartitions")
  void computedPartitionBoundsOverGroupsMatchReleasedFlink(String partition, String bound)
      throws Exception {
    for (boolean rank : new boolean[] {false, true}) {
      String sql =
          query(bound, rank, false)
              .replace("PARTITION BY k", "PARTITION BY " + partition)
              .replace("n DESC, id ASC", "n DESC, k ASC, id ASC");
      String hostPlan = environment(false).explainSql(sql);
      // Flink cannot carry the grouped upsert key through these computed partition keys.
      assertTrue(hostPlan.contains("RetractStrategy"), hostPlan);
      String plan = NativePlanner.explain(environment(false), sql);
      assertTrue(plan.contains("NativeColumnarTopN"), plan);
      NativeParity.assertOrderedKindedParity(() -> environment(false), sql);
      NativeParity.assertChangelogParity(() -> environment(false), sql);
      NativeParity.assertChangelogParity(() -> environment(true), sql);
    }
  }

  static Stream<Arguments> computedPartitions() {
    return Stream.of(
        Arguments.of("MOD(k, 3)", "MOD(k, 3) + 1"),
        Arguments.of("CAST(MOD(k, 3) AS SMALLINT)", "CAST(MOD(k, 3) AS SMALLINT) + 1"),
        Arguments.of("MOD(k, 3), MOD(id, 2)", "MOD(k, 3) + MOD(id, 2) + 1"));
  }

  static Stream<Arguments> orderedQueries() {
    return queries().filter(args -> !(boolean) args.get()[2]);
  }

  @ParameterizedTest
  @MethodSource("orderedQueries")
  void miniBatchRetainsPerPartitionBounds(String bound, boolean rank, boolean ties)
      throws Exception {
    // Group aggregation emits each bundle in map order. Break rank ties explicitly so the
    // materialized result is deterministic across engines; row-by-row tests cover tied arrivals.
    NativeParity.assertChangelogParity(() -> environment(true), query(bound, rank, ties));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void variableBoundsActuallyProcessNativeRows(boolean computedPartition) throws Exception {
    var reporter = InMemoryReporter.createWithRetainedMetrics();
    var cluster =
        new MiniClusterResource(
            new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(reporter.addToConfiguration(new Configuration()))
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .build());
    cluster.before();
    try {
      var table = environment(new TestStreamEnvironment(cluster.getMiniCluster(), 1), false);
      String sql = query("MOD(k, 3) + 1", true, false);
      if (computedPartition) sql = sql.replace("PARTITION BY k", "PARTITION BY MOD(k, 3)");
      String hostPlan = table.explainSql(sql);
      assertTrue(
          hostPlan.contains(computedPartition ? "RetractStrategy" : "UpdateFastStrategy"),
          hostPlan);
      var scan = NativePlanner.install(table);
      var result = table.executeSql(sql);
      try (var rows = result.collect()) {
        while (rows.hasNext()) rows.next();
      }
      assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
      var groups =
          reporter.findOperatorMetricGroups(
              result.getJobClient().orElseThrow().getJobID(), "(?i)NativeColumnarTopN");
      assertEquals(1, groups.size(), "the variable rank must execute natively");
      var metrics = reporter.getMetricsByGroup(groups.iterator().next());
      assertTrue(((Counter) metrics.get("numRecordsIn")).getCount() > 0);
      assertTrue(((Counter) metrics.get("numRecordsOut")).getCount() > 0);
      assertEquals(0, ((Counter) metrics.get("topn.invalidTopSize")).getCount());
    } finally {
      cluster.after();
    }
  }

  private static String query(String bound, boolean rank, boolean ties) {
    return "SELECT k, id, n, payload"
        + (rank ? ", rn" : "")
        + " FROM (SELECT k, id, n, payload, "
        + bound
        + " AS rank_end, ROW_NUMBER() OVER (PARTITION BY k ORDER BY n DESC"
        + (ties ? "" : ", id ASC")
        + ") AS rn FROM (SELECT k, id, COUNT(v) AS n, SUM(id) AS payload FROM src GROUP BY k, id))"
        + " WHERE rn <= rank_end";
  }

  private static TableEnvironment environment(boolean miniBatch) {
    return environment(StreamExecutionEnvironment.getExecutionEnvironment(), miniBatch);
  }

  private static TableEnvironment environment(StreamExecutionEnvironment env, boolean miniBatch) {
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    if (miniBatch) {
      table.getConfig().set("table.exec.mini-batch.enabled", "true");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
      table.getConfig().set("table.exec.mini-batch.size", "37");
    }
    List<Row> rows = new ArrayList<>();
    for (int round = 0; round < 4; round++) {
      for (long k : new long[] {-2, 0, 1, 2, 3, 200, Long.MAX_VALUE}) {
        for (long i = 0; i < 130; i++) {
          long id = round % 2 == 0 ? i : 129 - i;
          rows.add(Row.of(k, id, round == 1 && id % 3 == 0 ? null : (long) round));
        }
      }
    }
    var source =
        env.fromData(
            rows,
            Types.ROW_NAMED(new String[] {"k", "id", "v"}, Types.LONG, Types.LONG, Types.LONG));
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            source,
            Schema.newBuilder()
                .column("k", DataTypes.BIGINT().notNull())
                .column("id", DataTypes.BIGINT().notNull())
                .column("v", DataTypes.BIGINT())
                .build()));
    return table;
  }
}
