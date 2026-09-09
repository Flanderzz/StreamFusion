package tech.streamfusion;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

final class StringFunctionTestInputs {
  private StringFunctionTestInputs() {}

  static TableEnvironment search() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.createTemporaryView(
        "searches",
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"id", "s", "needle", "start_pos", "binary_value"},
                Types.INT,
                Types.STRING,
                Types.STRING,
                Types.INT,
                Types.PRIMITIVE_ARRAY(Types.BYTE)),
            Row.of(0, "abcabc", "bc", 3, new byte[] {(byte) 0xff, 0}),
            Row.of(1, "", "", -1, new byte[0]),
            Row.of(2, "abc", "", Integer.MAX_VALUE, null),
            Row.of(3, null, "a", 1, new byte[] {1}),
            Row.of(4, "abc", null, 1, null),
            Row.of(5, "abc", "abc", null, new byte[] {0}),
            Row.of(6, "a\ud83d\ude00\u4e2db\ud83d\ude00", "\ud83d\ude00", 3, new byte[0]),
            Row.of(7, "\u00e9e\u0301abc", "abc", 2, new byte[0]),
            Row.of(8, "a\u0000b\u0000", "\u0000", 3, new byte[] {0}),
            Row.of(9, "%_abc_%", "_%", 1, new byte[0]),
            Row.of(10, "\\abc\\", "\\", 0, new byte[0]),
            Row.of(11, "abc", "abcabc", Integer.MIN_VALUE, new byte[0]),
            Row.of(12, "abcabc", "bc", Integer.MIN_VALUE, new byte[0]),
            Row.of(13, "\u00e9".repeat(4097) + "abc", "abc", 4097, new byte[0])),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .column("needle", DataTypes.STRING())
            .column("start_pos", DataTypes.INT())
            .column("binary_value", DataTypes.BYTES())
            .build());
    return tEnv;
  }

}
