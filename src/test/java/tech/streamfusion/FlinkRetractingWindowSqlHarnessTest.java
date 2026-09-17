package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkRetractingWindowSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE,false", "TWO_PHASE,TUMBLE,false",
    "ONE_PHASE,HOP,false", "TWO_PHASE,HOP,false",
    "ONE_PHASE,CUMULATE,false", "TWO_PHASE,CUMULATE,false",
    "ONE_PHASE,TUMBLE,true", "TWO_PHASE,TUMBLE,true",
    "ONE_PHASE,HOP,true", "TWO_PHASE,HOP,true",
    "ONE_PHASE,CUMULATE,true", "TWO_PHASE,CUMULATE,true"
  })
  void updatingTopOneRequiresWindowRetractionAndGroupLiveness(
      String phase, String shape, boolean distinct) throws Exception {
    String window =
        switch (shape) {
          case "TUMBLE" -> "TUMBLE(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND)";
          case "HOP" ->
              "HOP(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '10' SECOND)";
          default ->
              "CUMULATE(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '15' SECOND)";
        };
    String sql =
        "SELECT k, window_start, window_end, "
            + (distinct ? "COUNT(DISTINCT v)" : "COUNT(v), SUM(v)")
            + " FROM TABLE("
            + window
            + ") GROUP BY k, window_start, window_end";
    List<Row> host = collect(environment(phase), sql);
    assertEquals(expected(shape, distinct), host, "unexpected released Flink result");

    TableEnvironment accelerated = environment(phase);
    var scan = NativePlanner.install(accelerated);
    assertEquals(host, collect(accelerated, sql), "raw window changelog differs from Flink");
    assertEquals(0, scan.substitutions(), "retracting window pipeline must stay on Flink");
    assertTrue(
        scan.fallbackReasons().stream()
            .anyMatch(
                reason ->
                    reason.contains(
                        "retracting or updating input requires retractable accumulators and group"
                            + " liveness")),
        scan.fallbackReasons().toString());
  }

  private static List<Row> expected(String shape, boolean distinct) {
    var rows = new ArrayList<Row>();
    if (shape.equals("HOP")) {
      rows.add(result(1, -5, 5, 20L, distinct));
      rows.add(result(3, -5, 5, null, distinct));
      rows.add(result(1, 0, 10, 20L, distinct));
      rows.add(result(2, 0, 10, 8L, distinct));
      rows.add(result(3, 0, 10, null, distinct));
      rows.add(result(2, 5, 15, 8L, distinct));
    } else {
      rows.add(result(1, 0, 5, 20L, distinct));
      rows.add(result(3, 0, 5, null, distinct));
      if (shape.equals("TUMBLE")) {
        rows.add(result(2, 5, 10, 8L, distinct));
      } else {
        for (int end : new int[] {10, 15}) {
          rows.add(result(1, 0, end, 20L, distinct));
          rows.add(result(2, 0, end, 8L, distinct));
          rows.add(result(3, 0, end, null, distinct));
        }
      }
    }
    rows.sort(Comparator.comparing(Row::toString));
    return rows;
  }

  private static Row result(int key, int start, int end, Long value, boolean distinct) {
    LocalDateTime left = LocalDateTime.ofEpochSecond(start, 0, ZoneOffset.UTC);
    LocalDateTime right = LocalDateTime.ofEpochSecond(end, 0, ZoneOffset.UTC);
    long count = value == null ? 0L : 1L;
    return distinct ? Row.of(key, left, right, count) : Row.of(key, left, right, count, value);
  }

  private static List<Row> collect(TableEnvironment table, String sql) throws Exception {
    var rows = new ArrayList<Row>();
    try (var iterator = table.executeSql(sql).collect()) {
      iterator.forEachRemaining(rows::add);
    }
    rows.sort(Comparator.comparing(Row::toString));
    return rows;
  }

  private static TableEnvironment environment(String phase) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    var source =
        env.fromData(
                Types.ROW_NAMED(
                    new String[] {"k", "millis", "v"}, Types.INT, Types.LONG, Types.LONG),
                Row.of(1, 1000L, 10L),
                Row.of(1, 2000L, 20L),
                Row.of(1, 3000L, 15L),
                Row.of(2, 4000L, 7L),
                Row.of(1, 2000L, 20L),
                Row.of(2, 6000L, 8L),
                Row.of(3, 2000L, null))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner((row, previous) -> (Long) row.getField(1)));
    table.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("millis", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    table.createTemporaryView(
        "ranked",
        table.sqlQuery(
            "SELECT k, rt, v FROM (SELECT k, rt, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v"
                + " DESC) AS rn FROM src) WHERE rn = 1"));
    return table;
  }
}
