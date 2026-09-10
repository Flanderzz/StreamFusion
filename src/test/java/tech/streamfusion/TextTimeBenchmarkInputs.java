package tech.streamfusion;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/** Source fixtures shared by each independent text/time benchmark and its identity control. */
final class TextTimeBenchmarkInputs {
  private TextTimeBenchmarkInputs() {}

  static String baselineExpression(String input) {
    return switch (input) {
      case "tt_bytes" -> "b";
      case "tt_timestamp" -> "ts";
      default -> "s";
    };
  }

  static String baselineType(String input) {
    return switch (input) {
      case "tt_bytes" -> "BYTES";
      case "tt_timestamp" -> "TIMESTAMP(9)";
      default -> "STRING";
    };
  }

  static TableEnvironment environment(
      String input, long rows, int bytes, boolean unicode, int nullEvery) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    String[] text = {
      payload(unicode ? " |\u4e2daB\ud83d\ude00| " : " |abCd| efGh| ", bytes),
      payload(unicode ? " |\u00e9dE\ud83d\ude42| " : " |deFg| abCd| ", bytes)
    };
    if (input.equals("tt_timestamp")) {
      LocalDateTime[] values = {
        LocalDateTime.of(1969, 12, 31, 23, 59, 59, 987654321),
        LocalDateTime.of(2000, 2, 29, 12, 34, 56, 123456789),
        LocalDateTime.of(2021, 1, 1, 0, 0)
      };
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(i -> Row.of(isNull(i, nullEvery) ? null : values[(int) (i % values.length)]))
              .returns(Types.ROW_NAMED(new String[] {"ts"}, Types.LOCAL_DATE_TIME)),
          Schema.newBuilder().column("ts", DataTypes.TIMESTAMP(9)).build());
    } else if (input.equals("tt_bytes")) {
      byte[][] values = {
        text[0].getBytes(StandardCharsets.UTF_8), text[1].getBytes(StandardCharsets.UTF_8)
      };
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(i -> Row.of(isNull(i, nullEvery) ? null : values[(int) (i % 2)]))
              .returns(Types.ROW_NAMED(new String[] {"b"}, Types.PRIMITIVE_ARRAY(Types.BYTE))),
          Schema.newBuilder().column("b", DataTypes.BYTES()).build());
    } else if (input.equals("tt_substring")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(
                  i ->
                      Row.of(
                          isNull(i, nullEvery) ? null : text[(int) (i % 2)],
                          i % 2 == 0 ? 3 : -16,
                          (int) (i % 3) * 8))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"s", "n", "len"}, Types.STRING, Types.INT, Types.INT)),
          Schema.newBuilder()
              .column("s", DataTypes.STRING())
              .column("n", DataTypes.INT())
              .column("len", DataTypes.INT())
              .build());
    } else if (input.equals("tt_counted")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(
                  i -> Row.of(isNull(i, nullEvery) ? null : text[(int) (i % 2)], (int) (i % 3) * 8))
              .returns(Types.ROW_NAMED(new String[] {"s", "n"}, Types.STRING, Types.INT)),
          Schema.newBuilder().column("s", DataTypes.STRING()).column("n", DataTypes.INT()).build());
    } else if (input.equals("tt_pad") || input.equals("tt_split")) {
      String[] pads =
          input.equals("tt_split")
              ? new String[] {"|", " "}
              : unicode
                  ? new String[] {"\u4e2d\ud83d\ude00", "\u00e9x"}
                  : new String[] {"xy", "ab"};
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(
                  i ->
                      Row.of(
                          isNull(i, nullEvery) ? null : text[(int) (i % 2)],
                          input.equals("tt_pad")
                              ? text[(int) (i % 2)].length() + 8 + (int) (i % 3)
                              : (int) (i % 3),
                          pads[(int) (i / 2 % 2)]))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"s", "n", "p"}, Types.STRING, Types.INT, Types.STRING)),
          Schema.newBuilder()
              .column("s", DataTypes.STRING())
              .column("n", DataTypes.INT())
              .column("p", DataTypes.STRING())
              .build());
    } else if (input.equals("tt_trim")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(
                  i ->
                      Row.of(
                          isNull(i, nullEvery) ? null : text[(int) (i % 2)],
                          i % 3 == 0 ? " |ab" : " |de"))
              .returns(Types.ROW_NAMED(new String[] {"s", "p"}, Types.STRING, Types.STRING)),
          Schema.newBuilder()
              .column("s", DataTypes.STRING())
              .column("p", DataTypes.STRING())
              .build());
    } else {
      String quotePattern = unicode ? "\u4e2d\\n\\t\\\"\\\\" : "ab\\n\\t\\\"\\\\";
      String quoted =
          "\""
              + quotePattern.repeat(bytes / quotePattern.getBytes(StandardCharsets.UTF_8).length)
              + "\"";
      String[] values =
          switch (input) {
            case "tt_text" -> text;
            case "tt_json_predicate" ->
                new String[] {
                  "{\"padding\":\"" + text[0] + "\"}",
                  "[\"" + text[1] + "\"]",
                  "\"" + text[0] + "\"",
                  "{\"invalid\":\"" + text[1] + "\",}"
                };
            case "tt_quoted" -> new String[] {quoted, quoted};
            case "tt_json_boolean", "tt_json_integer", "tt_json_double" -> {
              String[] selected =
                  switch (input) {
                    case "tt_json_boolean" -> new String[] {"true", "false"};
                    case "tt_json_integer" -> new String[] {"123456789", "-234567890"};
                    default -> new String[] {"1.23456789", "-2.3456789e12"};
                  };
              yield new String[] {
                "{\"v\":" + selected[0] + ",\"padding\":\"" + text[0] + "\"}",
                "{\"v\":" + selected[1] + ",\"padding\":\"" + text[1] + "\"}"
              };
            }
            case "tt_json" -> {
              int fields = Integer.getInteger("scalar.json.fields", 0);
              if (fields < 0) {
                throw new IllegalArgumentException("scalar.json.fields must be nonnegative");
              }
              StringBuilder members = new StringBuilder();
              for (int i = 0; i < fields; i++) {
                members.append(",\"field").append(i).append("\":\"value").append(i).append("\"");
              }
              yield new String[] {
                "{\"user\":{\"name\":\""
                    + (unicode ? "\u4e2d\\n\ud83d\ude00" : "Alice")
                    + "\",\"active\":true}"
                    + members
                    + ",\"padding\":\""
                    + text[0]
                    + "\"}",
                "{\"user\":{\"active\":false}" + members + ",\"padding\":\"" + text[1] + "\"}"
              };
            }
            case "tt_date_text" -> new String[] {"2000-02-29", "1969-12-31"};
            default -> throw new IllegalArgumentException("Unknown text/time input: " + input);
          };
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(i -> Row.of(isNull(i, nullEvery) ? null : values[(int) (i % values.length)]))
              .returns(Types.ROW_NAMED(new String[] {"s"}, Types.STRING)),
          Schema.newBuilder().column("s", DataTypes.STRING()).build());
    }
    return tables;
  }

  private static boolean isNull(long row, int nullEvery) {
    return nullEvery > 0 && row % nullEvery == 0;
  }

  private static String payload(String pattern, int bytes) {
    int size = pattern.getBytes(StandardCharsets.UTF_8).length;
    return pattern.repeat(bytes / size) + "x".repeat(bytes % size);
  }
}
