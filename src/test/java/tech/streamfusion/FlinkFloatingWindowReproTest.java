package tech.streamfusion;

import java.time.LocalDateTime;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkFloatingWindowReproTest {
  @Test
  void tumbleNanFirst() throws Exception {
    check("TUMBLE", Double.NaN, 2.0);
  }

  @Test
  void tumbleNanLast() throws Exception {
    check("TUMBLE", 2.0, Double.NaN);
  }

  @Test
  void tumblePositiveZeroFirst() throws Exception {
    check("TUMBLE", 0.0, -0.0);
  }

  @Test
  void hopNanFirst() throws Exception {
    check("HOP", Double.NaN, 2.0);
  }

  @Test
  void sessionNanFirst() throws Exception {
    check("SESSION", Double.NaN, 2.0);
  }

  @Test
  void finiteControl() throws Exception {
    check("TUMBLE", 3.0, 2.0);
  }

  private static void check(String function, double first, double second) throws Exception {
    String intervals = function.equals("HOP")
        ? "INTERVAL '1' SECOND, INTERVAL '2' SECOND" : "INTERVAL '1' SECOND";
    NativeParity.assertParity(() -> input(first, second),
        "SELECT window_start, MIN(d), MAX(d), MIN(f), MAX(f) FROM TABLE("
            + function + "(TABLE n, DESCRIPTOR(ts), " + intervals + ")) "
            + "GROUP BY window_start, window_end");
  }

  private static TableEnvironment input(double first, double second) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneId.of("UTC"));
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    LocalDateTime ts = LocalDateTime.of(2026, 1, 1, 0, 0);
    table.createTemporaryView("n", env.fromData(
        Types.ROW_NAMED(new String[] {"ts", "d", "f"},
            Types.LOCAL_DATE_TIME, Types.DOUBLE, Types.FLOAT),
        Row.of(ts, first, (float) first), Row.of(ts, second, (float) second)),
        Schema.newBuilder().column("ts", DataTypes.TIMESTAMP(3))
            .column("d", DataTypes.DOUBLE()).column("f", DataTypes.FLOAT())
            .watermark("ts", "ts - INTERVAL '1' SECOND").build());
    return table;
  }
}
