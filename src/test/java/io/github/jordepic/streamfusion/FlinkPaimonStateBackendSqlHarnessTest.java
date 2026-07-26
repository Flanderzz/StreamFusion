package io.github.jordepic.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

/**
 * The Paimon state backend behind Flink's normal backend toggle: with {@code state.backend.type}
 * set to the StreamFusion factory, a native group aggregate keeps its state in a local Paimon
 * table (read-through probes, barrier commits) and must produce exactly the host's results; host
 * (fallback) operators in the same job run unchanged on the wrapped hashmap backend. MIN/MAX keep
 * multiset state, which the Paimon row codec does not carry — that query exercises the
 * per-operator fallback to memory state under the same backend.
 */
class FlinkPaimonStateBackendSqlHarnessTest {

  // Collapsed-changelog parity: the bounded filesystem source may split the input across part
  // files whose read order differs run to run, so the raw -U/+U interleaving is not stable here.
  // Per-row changelog parity on the Paimon backend is covered deterministically by the operator
  // harness test; this verifies the materialized end state through the whole SQL stack.

  @Test
  void groupBySumOnPaimonBackendMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("paimon-sum-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        () -> paimonEnvironment(input), "SELECT k, SUM(v) AS total, COUNT(*) AS c FROM t GROUP BY k");
  }

  @Test
  void proctimeDeduplicationOnPaimonBackendMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("paimon-dedup-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        () -> paimonEnvironment(input),
        "SELECT k, v FROM (SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY PROCTIME() DESC)"
            + " AS rn FROM t) WHERE rn = 1");
  }

  @Test
  void retractingTopNOnPaimonBackendMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("paimon-retopn-in");
    writeInput(input);
    // A Top-N over a GROUP BY changelog plans as the retracting ranker; both stateful operators
    // in this job keep their state in Paimon tables.
    NativeParity.assertChangelogParity(
        () -> paimonEnvironment(input),
        "SELECT k, total FROM (SELECT k, total, ROW_NUMBER() OVER (ORDER BY total DESC) AS rn"
            + " FROM (SELECT k, SUM(v) AS total FROM t GROUP BY k)) WHERE rn <= 2");
  }

  @Test
  void rowtimeKeepFirstDeduplicationOnPaimonBackendMatchesHost() throws Exception {
    // Guard against a silent fallback first: the dedup row type includes the rowtime column,
    // which the bridge pins to a nanosecond timestamp — if the backend's type map refused it the
    // parity below would pass vacuously on memory state.
    org.apache.flink.table.types.logical.RowType dedupRow =
        org.apache.flink.table.types.logical.RowType.of(
            new org.apache.flink.table.types.logical.BigIntType(),
            new org.apache.flink.table.types.logical.BigIntType(),
            new org.apache.flink.table.types.logical.BigIntType(),
            new org.apache.flink.table.types.logical.LocalZonedTimestampType(3));
    try (org.apache.arrow.memory.BufferAllocator allocator =
            new org.apache.arrow.memory.RootAllocator();
        org.apache.arrow.c.ArrowSchema schema =
            org.apache.arrow.c.ArrowSchema.allocateNew(allocator)) {
      org.apache.arrow.c.Data.exportSchema(
          allocator,
          io.github.jordepic.streamfusion.arrow.ArrowConversion.toArrowSchema(dedupRow),
          null,
          schema);
      org.junit.jupiter.api.Assertions.assertTrue(
          Native.paimonRowStateSupported(schema.memoryAddress()),
          "the keep-first dedup row type must be persistable on the Paimon backend");
    }
    // Watermark-driven keep-first: candidates and fired markers live in the Paimon store; every
    // watermark firing is a range read merging the uncommitted write buffer with the committed
    // table, checkpointing every 50 ms so both sides of that merge are exercised.
    NativeParity.assertParity(
        FlinkPaimonStateBackendSqlHarnessTest::paimonRowtimeEnvironment,
        "SELECT k, v, ts FROM ("
            + "SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt ASC) AS rn FROM src)"
            + " WHERE rn = 1");
  }

  @Test
  void rowtimeKeepLastDeduplicationOnPaimonBackendMatchesHost() throws Exception {
    // Rowtime keep-last rows carry the rowtime column too (nanosecond timestamps after the
    // bridge), so persisting them rides the same type-map support as keep-first.
    NativeParity.assertChangelogParity(
        FlinkPaimonStateBackendSqlHarnessTest::paimonRowtimeEnvironment,
        "SELECT k, v, ts FROM ("
            + "SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt DESC) AS rn FROM src)"
            + " WHERE rn = 1");
  }

  @Test
  void rowtimeOverAggregateOnPaimonBackendMatchesHost() throws Exception {
    // Guard against a silent fallback: the OVER input row carries the rowtime column (pinned to
    // nanoseconds by the bridge) and the running-SUM fold state must be persistable too.
    org.apache.flink.table.types.logical.RowType overRow =
        org.apache.flink.table.types.logical.RowType.of(
            new org.apache.flink.table.types.logical.BigIntType(),
            new org.apache.flink.table.types.logical.BigIntType(),
            new org.apache.flink.table.types.logical.BigIntType(),
            new org.apache.flink.table.types.logical.LocalZonedTimestampType(3));
    try (org.apache.arrow.memory.BufferAllocator allocator =
            new org.apache.arrow.memory.RootAllocator();
        org.apache.arrow.c.ArrowSchema schema =
            org.apache.arrow.c.ArrowSchema.allocateNew(allocator)) {
      org.apache.arrow.c.Data.exportSchema(
          allocator,
          io.github.jordepic.streamfusion.arrow.ArrowConversion.toArrowSchema(overRow),
          null,
          schema);
      org.junit.jupiter.api.Assertions.assertTrue(
          Native.paimonOverStateSupported(
              schema.memoryAddress(), new int[] {0}, new int[] {0}, 0, false),
          "the rowtime OVER state shape must be persistable on the Paimon backend");
    }
    // Watermark-driven OVER: pending rows and the per-key running fold live in the Paimon store;
    // every firing is a range read merging the write buffer with the committed table, and the
    // running sum crosses 50 ms barriers, so folds round-trip through the folds table.
    NativeParity.assertParity(
        FlinkPaimonStateBackendSqlHarnessTest::paimonRowtimeEnvironment,
        "SELECT k, v, ts, SUM(v) OVER (PARTITION BY k ORDER BY rt) AS s FROM src");
  }

  @Test
  void windowTopNOnPaimonBackendMatchesHost() throws Exception {
    // Event-time window Top-N: open windows' buffers stage into the Paimon table at each 50 ms
    // barrier, and every watermark firing merges the write buffer with a committed range scan
    // (a window buffered before a barrier and closed after it fires from the table).
    NativeParity.assertParity(
        FlinkPaimonStateBackendSqlHarnessTest::paimonRowtimeEnvironment,
        "SELECT k, v, window_start FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start,"
            + " window_end, k ORDER BY v DESC) AS rn FROM"
            + " TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND))) WHERE rn <= 2");
  }

  @Test
  void windowJoinOnPaimonBackendMatchesHost() throws Exception {
    // Event-time window join: both sides' rows buffer in per-side Paimon row-buffer tables
    // across 50 ms barriers, and every watermark firing joins each side's range read (write
    // buffer merged with the committed table) — a window buffered before a barrier and closed
    // after it joins from the tables.
    NativeParity.assertParity(
        FlinkPaimonStateBackendSqlHarnessTest::paimonRowtimeEnvironment,
        "SELECT a.k, a.v, b.v FROM "
            + "(SELECT * FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND))) a "
            + "JOIN "
            + "(SELECT * FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND))) b "
            + "ON a.k = b.k AND a.window_start = b.window_start AND a.window_end = b.window_end");
  }

  @Test
  void unsupportedAggregatesFallBackToMemoryStateUnderPaimonBackend() throws Exception {
    Path input = Files.createTempDirectory("paimon-minmax-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        () -> paimonEnvironment(input),
        "SELECT k, MIN(v) AS mn, MAX(v) AS mx, SUM(v) AS s FROM t GROUP BY k");
  }

  // Batch mode at parallelism 1 writes exactly one part file. The proctime dedup query is
  // arrival-order sensitive, and the filesystem source's read order across multiple part files is
  // not stable between the two parity runs — a streaming-mode write rolling files at checkpoints
  // made this suite flaky.
  private static void writeInput(Path directory) throws Exception {
    TableEnvironment tEnv =
        TableEnvironment.create(
            org.apache.flink.table.api.EnvironmentSettings.inBatchMode());
    tEnv.getConfig().set("parallelism.default", "1");
    tEnv.executeSql(
        "CREATE TABLE in_write (k BIGINT, v BIGINT) WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql(
            "INSERT INTO in_write VALUES (1, 10), (1, 20), (2, 5), (1, 30), (2, 15), (3, 7)")
        .await();
  }

  /** The rowtime dedup harness source (out-of-order rows per key) on the Paimon backend. */
  private static TableEnvironment paimonRowtimeEnvironment() {
    Configuration configuration = new Configuration();
    configuration.setString(
        "state.backend.type", "io.github.jordepic.streamfusion.state.PaimonStateBackendFactory");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(1);
    env.enableCheckpointing(50);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
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

  private static TableEnvironment paimonEnvironment(Path directory) {
    Configuration configuration = new Configuration();
    configuration.setString(
        "state.backend.type", "io.github.jordepic.streamfusion.state.PaimonStateBackendFactory");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(1);
    env.enableCheckpointing(50);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, v BIGINT) WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    return tEnv;
  }
}
