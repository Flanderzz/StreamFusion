package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
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

class FlinkWindowLateSliceSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE", "TWO_PHASE,TUMBLE",
    "ONE_PHASE,HOP", "TWO_PHASE,HOP",
    "ONE_PHASE,CUMULATE", "TWO_PHASE,CUMULATE"
  })
  void aClosedSliceCanStillContributeToOpenFinalWindows(String phase, String shape)
      throws Exception {
    String window =
        switch (shape) {
          case "TUMBLE" -> "TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '5' SECOND)";
          case "HOP" -> "HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '10' SECOND)";
          default ->
              "CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '15' SECOND)";
        };
    String sql =
        "SELECT k, window_end, SUM(v), COUNT(*), COUNT(DISTINCT v) FROM TABLE("
            + window
            + ") GROUP BY k, window_start, window_end";
    String plan = NativePlanner.explain(environment(phase), sql);
    assertTrue(
        plan.contains(
            phase.equals("TWO_PHASE")
                ? "NativeColumnarGlobalWindowAggregate"
                : "NativeColumnarWindowAggregate"),
        plan);
    List<List<Object>> expected = new ArrayList<>();
    expected.add(result(1, 5, 10, 1, 1));
    if (!shape.equals("TUMBLE")) {
      expected.add(result(1, 10, 31, 3, 2));
      expected.add(result(2, 10, 13, 1, 1));
    }
    if (shape.equals("CUMULATE")) {
      expected.add(result(1, 15, 48, 4, 3));
      expected.add(result(2, 15, 13, 1, 1));
    }
    NativeParity.assertKindedParity(() -> environment(phase), sql, expected);
  }

  private static List<Object> result(int key, int end, long sum, long count, long distinct) {
    return List.of(
        "+I", key, LocalDateTime.ofEpochSecond(end, 0, ZoneOffset.UTC), sum, count, distinct);
  }

  private static TableEnvironment environment(String phase) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    var source =
        env.fromData(
                Types.ROW_NAMED(
                    new String[] {"k", "millis", "v", "wm"},
                    Types.INT,
                    Types.LONG,
                    Types.LONG,
                    Types.LONG),
                Row.of(1, 4000L, 10L, 5000L),
                Row.of(1, 4000L, 10L, 6000L),
                Row.of(1, 4000L, 11L, 7000L),
                Row.of(2, 4000L, 13L, 10000L),
                Row.of(1, 4000L, 17L, 15000L),
                Row.of(1, 4000L, 99L, 20000L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forGenerator(
                        context ->
                            new WatermarkGenerator<Row>() {
                              @Override
                              public void onEvent(Row row, long timestamp, WatermarkOutput output) {
                                output.emitWatermark(new Watermark((Long) row.getField(3)));
                              }

                              @Override
                              public void onPeriodicEmit(WatermarkOutput output) {}
                            })
                    .withTimestampAssigner((row, previous) -> (Long) row.getField(1)));
    table.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("millis", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("wm", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return table;
  }
}
