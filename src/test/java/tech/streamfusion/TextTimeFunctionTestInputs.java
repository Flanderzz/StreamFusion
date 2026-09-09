package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

final class TextTimeFunctionTestInputs {
  private TextTimeFunctionTestInputs() {}

  static TableEnvironment parameters() {
    String[] text = {
      null, "", "abc", "a\ud83d\ude00\u4e2db", "\u00e9e\u0301", "a|b||", " a  b ", "a\u0000b"
    };
    String[] pads = {null, "", "xy", "|", "\ud83d\ude00x", " "};
    Integer[] counts = {null, -7, -1, 0, 1, 2, 3, 7, 19};
    List<Row> rows = new ArrayList<>();
    for (String value : text) {
      for (Integer count : counts) {
        for (String pad : pads) {
          rows.add(Row.of(rows.size(), value, count, pad));
        }
      }
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "inputs",
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"id", "s", "n", "p"},
                Types.INT,
                Types.STRING,
                Types.INT,
                Types.STRING),
            rows.toArray(Row[]::new)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .column("n", DataTypes.INT())
            .column("p", DataTypes.STRING())
            .build());
    return tables;
  }

  static TableEnvironment textRows(String... text) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < text.length; i++) {
      rows.add(Row.of(i, text[i]));
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "inputs",
        env.fromData(
            Types.ROW_NAMED(new String[] {"id", "s"}, Types.INT, Types.STRING),
            rows.toArray(Row[]::new)),
        Schema.newBuilder().column("id", DataTypes.INT()).column("s", DataTypes.STRING()).build());
    return tables;
  }

  static TableEnvironment bytes() {
    List<Row> rows = new ArrayList<>();
    rows.add(Row.of(0, null));
    rows.add(Row.of(1, new byte[0]));
    for (int i = 0; i < 256; i++) {
      rows.add(Row.of(rows.size(), new byte[] {(byte) i}));
      rows.add(Row.of(rows.size(), new byte[] {(byte) 0xed, (byte) i, (byte) 0x80}));
      rows.add(Row.of(rows.size(), new byte[] {(byte) 0xf0, (byte) i, (byte) 0x80}));
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "inputs",
        env.fromData(
            Types.ROW_NAMED(new String[] {"id", "b"}, Types.INT, Types.PRIMITIVE_ARRAY(Types.BYTE)),
            rows.toArray(Row[]::new)),
        Schema.newBuilder().column("id", DataTypes.INT()).column("b", DataTypes.BYTES()).build());
    return tables;
  }
}
