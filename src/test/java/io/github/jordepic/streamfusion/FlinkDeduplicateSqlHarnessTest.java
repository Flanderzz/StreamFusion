package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/**
 * Row-time deduplication: per key the native operator keeps either the minimum-rowtime row
 * (keep-first, {@code ORDER BY rt ASC} — insert-only, emitted on the watermark) or the
 * maximum-rowtime row (keep-last, {@code ORDER BY rt DESC} — a retract changelog, emitted eagerly).
 */
class FlinkDeduplicateSqlHarnessTest {

  private static final String KEEP_FIRST =
      "SELECT k, v, rt FROM ("
          + "SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt ASC) AS rn FROM src) WHERE rn = 1";

  private static final String KEEP_LAST =
      "SELECT k, v, rt FROM ("
          + "SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt DESC) AS rn FROM src) WHERE rn = 1";

  @Test
  void keepFirstDeduplicationMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkDeduplicateSqlHarnessTest::environment, KEEP_FIRST);
  }

  @Test
  void keepLastDeduplicationMatchesHost() throws Exception {
    // Keep-last keeps the maximum-rowtime row per key and emits a retract changelog as a later row
    // displaces the stored one; the collapsed result is key 1's (v=30, rt=2000) and key 2's
    // (v=50, rt=1500).
    NativeParity.assertChangelogParity(FlinkDeduplicateSqlHarnessTest::environment, KEEP_LAST);
  }

  // Proctime dedup orders by arrival (no rowtime). Only k,v are projected (the PROCTIME() column is
  // wall-clock, hence non-deterministic) so the comparison is deterministic at parallelism 1.
  private static final String KEEP_FIRST_PROCTIME =
      "SELECT k, v FROM ("
          + "SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY pt ASC) AS rn FROM src) WHERE rn = 1";

  private static final String KEEP_LAST_PROCTIME =
      "SELECT k, v FROM ("
          + "SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY pt DESC) AS rn FROM src) WHERE rn = 1";

  @Test
  void keepFirstProctimeDeduplicationMatchesHost() throws Exception {
    // Proctime keep-first emits each key's first-arriving row (insert-only): key 1's (v=30), key 2's
    // (v=50) — by source/arrival order, not rowtime.
    NativeParity.assertParity(
        FlinkDeduplicateSqlHarnessTest::proctimeEnvironment, KEEP_FIRST_PROCTIME);
  }

  @Test
  void keepLastProctimeDeduplicationMatchesHost() throws Exception {
    // Proctime keep-last keeps each key's last-arriving row, emitting a retract changelog; the
    // collapsed result is key 1's (v=25) and key 2's (v=40).
    NativeParity.assertChangelogParity(
        FlinkDeduplicateSqlHarnessTest::proctimeEnvironment, KEEP_LAST_PROCTIME);
  }

  private static TableEnvironment proctimeEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    // Two identical consecutive rows, placed to pin Flink's proctime keep-last suppression
    // exactly: key 2's repeated first row is suppressed with TTL off (the stored row's kind is
    // still INSERT, so the kind-sensitive equaliser sees it as equal), while key 1's repeated
    // (v=25) row lands after an update — the stored row was mutated to UPDATE_AFTER on emission
    // (Flink's heap-state aliasing) — so it emits an identical -U/+U pair even with TTL off.
    // With TTL on the suppression is disabled and both duplicates emit.
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
            Row.of(1L, 30L),
            Row.of(2L, 50L),
            Row.of(2L, 50L),
            Row.of(1L, 20L),
            Row.of(2L, 40L),
            Row.of(1L, 25L),
            Row.of(1L, 25L));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .columnByExpression("pt", "PROCTIME()")
            .build());
    return tEnv;
  }

  @Test
  void stateTtlKeepLastEmitsUnsuppressedUpdatesAndMatchesHost() throws Exception {
    // With idle-state TTL on (1h — nothing expires in-test), Flink disables the identical-row
    // suppression: key 2's repeated first row produces an identical -U/+U pair the TTL-off run
    // would swallow. The kinded compare is the only one that can see such a pair, so this pins
    // the native TTL emission semantics change for change against the host.
    NativeParity.assertKindedParity(
        () -> {
          TableEnvironment tEnv = proctimeEnvironment();
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        KEEP_LAST_PROCTIME);
  }

  @Test
  void stateTtlOffSuppressesIdenticalProctimeRowsAndMatchesHost() throws Exception {
    // The TTL-off counterpart pins Flink's kind-sensitive suppression on both sides of the line:
    // key 2's repeated first row is swallowed, key 1's post-update duplicate is not (see the
    // source comment).
    NativeParity.assertKindedParity(
        FlinkDeduplicateSqlHarnessTest::proctimeEnvironment, KEEP_LAST_PROCTIME);
  }

  @Test
  void stateTtlKeepFirstProctimeMatchesHost() throws Exception {
    // Proctime keep-first runs TTL natively too (nothing expires in-test at 1h; each key still
    // emits exactly its first row).
    NativeParity.assertKindedParity(
        () -> {
          TableEnvironment tEnv = proctimeEnvironment();
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        KEEP_FIRST_PROCTIME);
  }

  @Test
  void stateTtlKeepLastRowtimeMatchesHost() throws Exception {
    // Rowtime keep-last runs TTL natively as well; Flink's rowtime variant never suppresses, so
    // the kinded changelog is identical with or without retention.
    NativeParity.assertKindedParity(
        () -> {
          TableEnvironment tEnv = environment();
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        KEEP_LAST);
  }

  @Test
  void stateTtlRowtimeKeepFirstMatchesHost() throws Exception {
    // The watermark-buffered rowtime keep-first runs TTL natively too (nothing expires in-test at
    // 1h): only the emitted markers are TTL'd — the buffered candidates mirror Flink's
    // deliberately un-TTL'd timer state — so each key still emits exactly its minimum-rowtime row.
    // Append-only output, deterministic at parallelism 1.
    NativeParity.assertKindedParity(
        () -> {
          TableEnvironment tEnv = environment();
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        KEEP_FIRST);
  }

  @Test
  void miniBatchKeepLastEmitsEveryKeptIntermediateAndMatchesHost() throws Exception {
    // Under mini-batch, Flink's rowtime keep-last emits a transition for EVERY row of the bundle
    // that displaces the kept row ("we output all changelog here rather than comparing the first
    // and the last record in buffer" — RowTimeMiniBatchDeduplicateFunction), not one net change
    // per key. That makes the kinded multiset bundle-boundary-invariant, so the compare is sound
    // even though the assigner's watermark cadence is wall-clock-driven.
    NativeParity.assertKindedParity(
        FlinkDeduplicateSqlHarnessTest::miniBatchEnvironment, KEEP_LAST);
  }

  @Test
  void miniBatchKeepLastWithStateTtlMatchesHost() throws Exception {
    // The rowtime path has no equality check, so retention changes nothing about the emitted
    // chain (only expiry itself, which the 1h retention keeps out of the test window).
    NativeParity.assertKindedParity(
        () -> {
          TableEnvironment tEnv = miniBatchEnvironment();
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        KEEP_LAST);
  }

  @Test
  void miniBatchCompactChangesNetsEachBundleAndMatchesHost() throws Exception {
    // With compact-changes on, Flink emits only each bundle's net transition per key
    // (RowTimeMiniBatchLatestChangeDeduplicateFunction), so the kinded multiset DEPENDS on the
    // bundle boundaries; the fixture pins them deterministically (see compactChangesEnvironment):
    // bundle one nets key 1's improving pair to a single +I, bundle two nets key 1's win over the
    // durable row to one -U/+U and displaces key 2 on an equal rowtime.
    NativeParity.assertKindedParity(
        FlinkDeduplicateSqlHarnessTest::compactChangesEnvironment, KEEP_LAST);
  }

  @Test
  void miniBatchCompactChangesWithStateTtlMatchesHost() throws Exception {
    // Compact-changes has no equality check either, so retention changes nothing about the netted
    // transitions (nothing expires in-test at 1h).
    NativeParity.assertKindedParity(
        () -> {
          TableEnvironment tEnv = compactChangesEnvironment();
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        KEEP_LAST);
  }

  /**
   * The mini-batch environment with compact-changes on and deterministic bundle boundaries: a
   * count trigger of 3 splits the six rows into exactly two bundles on the host and the native
   * runs alike (with timestamps at most 500, the 2s-interval rowtime assigner forwards no
   * mid-stream watermark that could cut a bundle short — only the end-of-input one).
   */
  private static TableEnvironment compactChangesEnvironment() {
    TableEnvironment tEnv = miniBatchEnvironment();
    tEnv.getConfig().set("table.exec.deduplicate.mini-batch.compact-changes-enabled", "true");
    tEnv.getConfig().set("table.exec.mini-batch.size", "3");
    return tEnv;
  }

  @Test
  void miniBatchRowtimeKeepFirstKeepsDeduplicateOnHost() throws Exception {
    // Under mini-batch, Flink's rowtime keep-first is the bundled retracting function (a
    // smaller-rowtime row displaces with -U/+U), not the insert-only watermark-buffered one the
    // native operator implements, so it must decline.
    assertDeduplicateFallsBack(miniBatchEnvironment(), KEEP_FIRST, "keep-first");
  }

  /** Runs the query with the native planner installed and asserts the dedup declined as stated. */
  private static void assertDeduplicateFallsBack(
      TableEnvironment tEnv, String sql, String expectedReason) throws Exception {
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    try (CloseableIterator<Row> rows = tEnv.executeSql(sql).collect()) {
      while (rows.hasNext()) {
        rows.next();
      }
    }
    assertTrue(
        scan.fallbackReasons().stream()
            .anyMatch(reason -> reason.startsWith("deduplicate:") && reason.contains(expectedReason)),
        "no deduplicate fallback reason contained \""
            + expectedReason
            + "\"; reasons="
            + scan.fallbackReasons());
  }

  /**
   * Mini-batch on, with several updates per key so one bundle holds a whole improving chain for
   * key 1 (plus a non-improving row and an equal-rowtime displacement for key 2).
   */
  private static TableEnvironment miniBatchEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(new String[] {"k", "v", "ts"}, Types.LONG, Types.LONG, Types.LONG),
                Row.of(1L, 10L, 100L),
                Row.of(2L, 7L, 500L),
                Row.of(1L, 20L, 200L),
                Row.of(1L, 15L, 150L),
                Row.of(2L, 9L, 500L),
                Row.of(1L, 30L, 300L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(2)));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    tEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
    tEnv.getConfig().set("table.exec.mini-batch.allow-latency", "2 s");
    tEnv.getConfig().set("table.exec.mini-batch.size", "100");
    return tEnv;
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // Multiple rows per key, out of order, so "first by rowtime" is not "first to arrive": key 1's
    // minimum-rowtime row is (v=20, rt=0); key 2's is (v=40, rt=1000).
    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(new String[] {"k", "v", "ts"}, Types.LONG, Types.LONG, Types.LONG),
                Row.of(1L, 30L, 2000L),
                Row.of(2L, 50L, 1500L),
                Row.of(1L, 20L, 0L),
                Row.of(2L, 40L, 1000L),
                Row.of(1L, 25L, 800L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(2)));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }
}
