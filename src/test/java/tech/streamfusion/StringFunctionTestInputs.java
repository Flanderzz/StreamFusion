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

  static TableEnvironment encodings() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    long[] numbers = {
      0,
      1,
      -1,
      Long.MIN_VALUE,
      Long.MAX_VALUE,
      Integer.MIN_VALUE,
      Integer.MAX_VALUE,
      Short.MIN_VALUE,
      Short.MAX_VALUE,
      Byte.MIN_VALUE,
      Byte.MAX_VALUE,
      255,
      256,
      4294967297L,
      -4294967297L,
      17,
      31,
      32
    };
    String[] strings = {
      "",
      "a",
      "ab",
      "abc",
      "abcd",
      "\u4e2d\ud83d\ude00",
      "\u00e9e\u0301",
      "a\u0000b",
      "line\r\nnext",
      " ",
      "\t",
      "%_\\",
      "x".repeat(57),
      "x".repeat(58),
      "\u4e2d".repeat(4097),
      null,
      "abc ",
      "0123456789"
    };
    String[] hexStrings = {
      "",
      "A",
      "AB",
      "ABC",
      "fF00",
      "12345",
      "G",
      "0G",
      "G12",
      " 12",
      "12\n",
      "0x12",
      "\uff11\uff12",
      "\u0000",
      "aF".repeat(4097),
      null,
      "f",
      "0123456789aBcDeF"
    };
    List<Row> rows = new ArrayList<>();
    for (int id = 0; id < numbers.length; id++) {
      long value = numbers[id];
      boolean isNull = id == 15;
      rows.add(
          Row.of(
              id,
              strings[id],
              hexStrings[id],
              isNull ? null : value,
              isNull ? null : (int) value,
              isNull ? null : (short) value,
              isNull ? null : (byte) value));
    }
    tEnv.createTemporaryView(
        "encodings",
        env.fromData(
            rows,
            Types.ROW_NAMED(
                new String[] {"id", "s", "hex_text", "n", "i", "sh", "t"},
                Types.INT,
                Types.STRING,
                Types.STRING,
                Types.LONG,
                Types.INT,
                Types.SHORT,
                Types.BYTE)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .column("hex_text", DataTypes.STRING())
            .column("n", DataTypes.BIGINT())
            .column("i", DataTypes.INT())
            .column("sh", DataTypes.SMALLINT())
            .column("t", DataTypes.TINYINT())
            .build());
    return tEnv;
  }

}
