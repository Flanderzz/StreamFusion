package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

/** Per-function end-to-end diagnostics, with source-matched identity controls. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class ScalarFunctionBenchmark {
  private static final long ROWS = Long.getLong("scalar.rows", 2_000_000L);
  private static final int BYTES = Integer.getInteger("scalar.bytes", 264);
  private static final int WARMUP = Integer.getInteger("scalar.warmup", 2);
  private static final int RUNS = Integer.getInteger("scalar.runs", 5);
  private static final boolean UNICODE = Boolean.getBoolean("scalar.unicode");
  private static final int NULL_EVERY = Integer.getInteger("scalar.nullEvery", 0);
  private static final String ENGINE =
      System.getProperty("scalar.engine", "both").toLowerCase(Locale.ROOT);

  private record Query(String name, String input, String expression, String outputType) {
    Query(String name, String input, String expression) {
      this(name, input, expression, input.equals("numbers") ? "BIGINT" : "STRING");
    }

    String sql() {
      return "INSERT INTO sink SELECT " + expression + ", TRUE FROM inputs";
    }

    String ddl() {
      return "CREATE TABLE sink (v "
          + outputType()
          + ", anchor BOOLEAN) WITH ('connector' = 'blackhole')";
    }
  }

  private static final List<Query> SCALAR_FUNCTIONS =
      List.of(
          new Query("GREATEST", "numbers", "GREATEST(n, m, 17)"),
          new Query("LEAST", "numbers", "LEAST(n, m, 17)"),
          new Query("INITCAP", "text", "INITCAP(s)"),
          new Query("TRANSLATE", "text", "TRANSLATE(s, 'abcdef', 'ABCDEF')"),
          new Query("BTRIM", "text", "BTRIM(s)"),
          new Query("ELT", "elt", "ELT(i, s, t)"),
          new Query("URL_ENCODE", "text", "URL_ENCODE(s)"),
          new Query("URL_DECODE", "encoded", "URL_DECODE(s)"),
          new Query("OVERLAY", "overlay", "OVERLAY(s PLACING t FROM i FOR n)"));

  private static final List<Query> SEARCH_FUNCTIONS =
      List.of(
          new Query("STARTSWITH_LITERAL", "search", "STARTSWITH(s, 'row:')", "BOOLEAN"),
          new Query("STARTSWITH_COLUMN", "search_prefix", "STARTSWITH(s, needle)", "BOOLEAN"),
          new Query("ENDSWITH_LITERAL", "search", "ENDSWITH(s, ':match')", "BOOLEAN"),
          new Query("ENDSWITH_COLUMN", "search_needle", "ENDSWITH(s, needle)", "BOOLEAN"),
          new Query("INSTR_LITERAL", "search", "INSTR(s, ':match')", "INT"),
          new Query("INSTR_COLUMN", "search_needle", "INSTR(s, needle)", "INT"),
          new Query("LOCATE2_LITERAL", "search", "LOCATE(':match', s)", "INT"),
          new Query("LOCATE2_COLUMN", "search_needle", "LOCATE(needle, s)", "INT"),
          new Query("LOCATE3_LITERAL", "search_start", "LOCATE(':match', s, start_pos)", "INT"),
          new Query(
              "LOCATE3_COLUMN", "search_needle_start", "LOCATE(needle, s, start_pos)", "INT"));

  private static final List<Query> ENCODING_FUNCTIONS =
      Stream.concat(
              Stream.of("tinyint", "smallint", "integer", "bigint")
                  .flatMap(
                      type ->
                          Stream.of(
                              new Query("BIN_" + type.toUpperCase(Locale.ROOT), type, "BIN(n)"),
                              new Query("HEX_" + type.toUpperCase(Locale.ROOT), type, "HEX(n)"))),
              Stream.of(
                  new Query("HEX_STRING", "text", "HEX(s)"),
                  new Query("TO_BASE64", "text", "TO_BASE64(s)"),
                  new Query("UNHEX", "hex", "UNHEX(s)", "BYTES")))
          .toList();

  private static final List<Query> FUNCTIONS =
      Stream.of(SCALAR_FUNCTIONS, SEARCH_FUNCTIONS, ENCODING_FUNCTIONS, TextTimeFunctions.QUERIES)
          .flatMap(List::stream)
          .toList();

  @Test
  void individualFunctions() throws Exception {
    if (ROWS < 1 || BYTES < 0 || WARMUP < 0 || RUNS < 1 || NULL_EVERY < 0) {
      throw new IllegalArgumentException("Invalid scalar benchmark sizes/trial counts");
    }
    if (!List.of("both", "flink", "native").contains(ENGINE)) {
      throw new IllegalArgumentException("scalar.engine must be both, flink, or native");
    }
    List<Query> selected = selected();
    Map<String, Query> baselines = new LinkedHashMap<>();
    for (Query query : selected) {
      if (query.input().startsWith("tt_")) {
        baselines.putIfAbsent(
            query.input(),
            new Query(
                "BASELINE_" + query.input(),
                query.input(),
                TextTimeBenchmarkInputs.baselineExpression(query.input()),
                TextTimeBenchmarkInputs.baselineType(query.input())));
        continue;
      }
      boolean integer = isIntegerInput(query.input());
      baselines.putIfAbsent(
          query.input(),
          new Query(
              "BASELINE_" + query.input(),
              query.input(),
              integer || query.input().equals("numbers") ? "n" : "s",
              integer
                  ? query.input().toUpperCase(Locale.ROOT)
                  : query.input().equals("numbers") ? "BIGINT" : "STRING"));
    }
    List<Query> queries = new ArrayList<>(baselines.values());
    queries.addAll(selected);
    List<String> csv =
        new ArrayList<>(
            List.of(
                "function,input,output_type,payload_bytes,unicode,null_every,rows,engine,trial,seconds"));
    Path output = Path.of(System.getProperty("scalar.output", "target/scalar-functions.csv"));
    if (output.getParent() != null && !Files.isDirectory(output.getParent())) {
      Files.createDirectories(output.getParent());
    }
    for (Query query : queries) {
      assertPlan(query);
      double[][] times = new double[2][RUNS];
      for (int trial = 0; trial < WARMUP + RUNS; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) % 2;
          if (ENGINE.equals(engine == 1 ? "flink" : "native")) {
            continue;
          }
          double seconds = run(query, engine == 1);
          if (trial >= WARMUP) {
            times[engine][trial - WARMUP] = seconds;
            csv.add(
                String.format(
                    Locale.ROOT,
                    "%s,%s,%s,%d,%s,%d,%d,%s,%d,%.6f",
                    query.name(),
                    query.input(),
                    query.outputType(),
                    BYTES,
                    UNICODE,
                    NULL_EVERY,
                    ROWS,
                    engine == 1 ? "native" : "flink",
                    trial - WARMUP,
                    seconds));
          }
        }
      }
      double host = median(times[0]);
      double nativeTime = median(times[1]);
      if (ENGINE.equals("both")) {
        System.out.printf(
            Locale.ROOT,
            "[scalar] %s bytes=%d rows=%d Flink median=%.3fs %s Native median=%.3fs %s"
                + " ratio=%.3fx%n",
            query.name(),
            BYTES,
            ROWS,
            host,
            Arrays.toString(times[0]),
            nativeTime,
            Arrays.toString(times[1]),
            host / nativeTime);
      } else {
        int engine = ENGINE.equals("native") ? 1 : 0;
        System.out.printf(
            Locale.ROOT,
            "[scalar] %s bytes=%d rows=%d engine=%s median=%.3fs %s%n",
            query.name(),
            BYTES,
            ROWS,
            ENGINE,
            median(times[engine]),
            Arrays.toString(times[engine]));
      }
      Files.write(output, csv);
    }
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE_SCALAR", matches = "true")
  void profileFunction() throws Exception {
    Query query = selected().get(0);
    boolean useNative = Boolean.parseBoolean(System.getProperty("scalar.native", "true"));
    assertPlan(query);
    for (int warmup = 0; warmup < WARMUP; warmup++) {
      run(query, useNative);
    }
    System.out.printf(
        "[scalar-profile] ready pid=%d function=%s native=%s%n",
        ProcessHandle.current().pid(), query.name(), useNative);
    String profiler = System.getProperty("profile.asprof", "asprof");
    Path directory = Path.of(System.getProperty("profile.outputDir", "target/profiles/scalar"));
    Files.createDirectories(directory);
    Path recording =
        directory
            .resolve(
                query.name().toLowerCase(Locale.ROOT) + (useNative ? "-native.jfr" : "-flink.jfr"))
            .toAbsolutePath();
    profileCommand(
        profiler,
        "start",
        "-e",
        "cpu",
        "-i",
        "1ms",
        "-f",
        recording.toString(),
        Long.toString(ProcessHandle.current().pid()));
    try {
      long deadline = System.nanoTime() + Long.getLong("scalar.seconds", 45L) * 1_000_000_000L;
      do {
        run(query, useNative);
      } while (System.nanoTime() < deadline);
    } finally {
      profileCommand(profiler, "stop", Long.toString(ProcessHandle.current().pid()));
    }
  }

  private static void profileCommand(String... command) throws Exception {
    int exit = new ProcessBuilder(command).inheritIO().start().waitFor();
    if (exit != 0) {
      throw new IllegalStateException("Profiler exited " + exit);
    }
  }

  private static List<Query> selected() {
    String names = System.getProperty("scalar.functions", "ALL").toUpperCase(Locale.ROOT);
    Map<String, Query> queries = new LinkedHashMap<>();
    for (String name : names.split(",", -1)) {
      List<Query> matches =
          switch (name.trim()) {
            case "ALL" -> FUNCTIONS;
            case "SCALAR" -> SCALAR_FUNCTIONS;
            case "SEARCH" -> SEARCH_FUNCTIONS;
            case "ENCODING" -> ENCODING_FUNCTIONS;
            case "TEXT_TIME" -> TextTimeFunctions.QUERIES;
            default -> FUNCTIONS.stream().filter(q -> q.name().equals(name.trim())).toList();
          };
      if (matches.isEmpty()) {
        throw new IllegalArgumentException("Unknown scalar.functions: " + name);
      }
      matches.forEach(query -> queries.putIfAbsent(query.name(), query));
    }
    return List.copyOf(queries.values());
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    int middle = sorted.length / 2;
    return sorted.length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
  }

  private static void assertPlan(Query query) {
    TableEnvironment tables = environment(query.input());
    tables.executeSql(query.ddl());
    String plan = NativePlanner.explain(tables, query.sql());
    if (!plan.contains("NativeCalc")
        || !plan.contains("RowDataToArrow")
        || !plan.contains("ArrowToRowData")) {
      throw new IllegalStateException(
          query.name() + " must include NativeCalc and both transposes: " + plan);
    }
  }

  private static double run(Query query, boolean useNative) throws Exception {
    TableEnvironment tables = environment(query.input());
    tables.executeSql(query.ddl());
    PhysicalPlanScan scan = useNative ? NativePlanner.install(tables) : null;
    long start = System.nanoTime();
    tables.executeSql(query.sql()).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (useNative && scan.substitutions() == 0) {
      throw new IllegalStateException(query.name() + " fell back: " + scan.fallbackReasons());
    }
    return seconds;
  }

  private static TableEnvironment environment(String input) {
    if (input.startsWith("tt_")) {
      return TextTimeBenchmarkInputs.environment(input, ROWS, BYTES, UNICODE, NULL_EVERY);
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    String[] text =
        UNICODE
            ? new String[] {
              payload(" \u4e2dAbC \ud83d\ude00dEf "), payload(" \u00e9dEf \ud83d\ude42AbC ")
            }
            : new String[] {payload(" abC def_09 "), payload(" dEf abc_90 ")};
    if (input.startsWith("search")) {
      String padding = payload(UNICODE ? "\u4e2d\ud83d\ude00x" : "x");
      String[] samples = {"row:" + padding + ":match", "other:" + padding + ":miss"};
      String[] needles =
          input.equals("search_prefix")
              ? new String[] {"row:", "other:"}
              : new String[] {":match", ":miss"};
      boolean columnNeedle = input.equals("search_prefix") || input.contains("needle");
      boolean start = input.endsWith("start");
      int step = padding.codePointCount(0, padding.length()) / 3;
      List<String> names = new ArrayList<>(List.of("s"));
      List<TypeInformation<?>> types = new ArrayList<>(List.of(Types.STRING));
      Schema.Builder schema = Schema.newBuilder().column("s", DataTypes.STRING());
      if (columnNeedle) {
        names.add("needle");
        types.add(Types.STRING);
        schema.column("needle", DataTypes.STRING());
      }
      if (start) {
        names.add("start_pos");
        types.add(Types.INT);
        schema.column("start_pos", DataTypes.INT());
      }
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, ROWS - 1)
              .map(
                  i -> {
                    Row row = new Row(1 + (columnNeedle ? 1 : 0) + (start ? 1 : 0));
                    row.setField(0, isNull(i) ? null : samples[(int) (i % 2)]);
                    if (columnNeedle) {
                      row.setField(1, needles[(int) (i / 2 % 2)]);
                    }
                    if (start) {
                      row.setField(columnNeedle ? 2 : 1, (int) (i % 3) * step + 1);
                    }
                    return row;
                  })
              .returns(
                  Types.ROW_NAMED(
                      names.toArray(String[]::new), types.toArray(TypeInformation<?>[]::new))),
          schema.build());
    } else if (isIntegerInput(input)) {
      TypeInformation<?> type =
          switch (input) {
            case "tinyint" -> Types.BYTE;
            case "smallint" -> Types.SHORT;
            case "integer" -> Types.INT;
            default -> Types.LONG;
          };
      DataType dataType =
          switch (input) {
            case "tinyint" -> DataTypes.TINYINT();
            case "smallint" -> DataTypes.SMALLINT();
            case "integer" -> DataTypes.INT();
            default -> DataTypes.BIGINT();
          };
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, ROWS - 1)
              .map(i -> Row.of(isNull(i) ? null : integerValue(i, input)))
              .returns(Types.ROW_NAMED(new String[] {"n"}, type)),
          Schema.newBuilder().column("n", dataType).build());
    } else if (input.equals("numbers")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, ROWS - 1)
              .map(i -> Row.of(isNull(i) ? null : i - ROWS / 2, ROWS / 3 - i))
              .returns(Types.ROW_NAMED(new String[] {"n", "m"}, Types.LONG, Types.LONG)),
          Schema.newBuilder()
              .column("n", DataTypes.BIGINT())
              .column("m", DataTypes.BIGINT())
              .build());
    } else if (input.equals("elt")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, ROWS - 1)
              .map(
                  i ->
                      Row.of(
                          isNull(i) ? null : text[(int) (i % 2)],
                          text[(int) ((i + 1) % 2)],
                          (int) (i % 2) + 1))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"s", "t", "i"}, Types.STRING, Types.STRING, Types.INT)),
          Schema.newBuilder()
              .column("s", DataTypes.STRING())
              .column("t", DataTypes.STRING())
              .column("i", DataTypes.INT())
              .build());
    } else if (input.equals("overlay")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, ROWS - 1)
              .map(
                  i -> Row.of(isNull(i) ? null : text[(int) (i % 2)], "ABC", (int) (i % 2) + 1, 2L))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"s", "t", "i", "n"},
                      Types.STRING,
                      Types.STRING,
                      Types.INT,
                      Types.LONG)),
          Schema.newBuilder()
              .column("s", DataTypes.STRING())
              .column("t", DataTypes.STRING())
              .column("i", DataTypes.INT())
              .column("n", DataTypes.BIGINT())
              .build());
    } else {
      String pattern = UNICODE ? "%E4%B8%AD+%F0%9F%98%80" : "a+b%2B%20";
      String encoded = pattern.repeat(Math.max(1, BYTES / pattern.length()));
      String[] hex = {"aF".repeat(BYTES), "01".repeat(BYTES)};
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, ROWS - 1)
              .map(
                  i ->
                      Row.of(
                          isNull(i)
                              ? null
                              : input.equals("encoded")
                                  ? encoded
                                  : input.equals("hex") ? hex[(int) (i % 2)] : text[(int) (i % 2)]))
              .returns(Types.ROW_NAMED(new String[] {"s"}, Types.STRING)),
          Schema.newBuilder().column("s", DataTypes.STRING()).build());
    }
    return tables;
  }

  private static final class TextTimeFunctions {
    static final List<Query> QUERIES =
        List.of();
  }

  private static String payload(String pattern) {
    int bytes = pattern.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    return pattern.repeat(BYTES / bytes) + "x".repeat(BYTES % bytes);
  }

  private static boolean isIntegerInput(String input) {
    return List.of("tinyint", "smallint", "integer", "bigint").contains(input);
  }

  private static Number integerValue(long row, String input) {
    long value = row % 2 == 0 ? row / 2 : -(row / 2) - 1;
    return switch (input) {
      case "tinyint" -> (byte) value;
      case "smallint" -> (short) value;
      case "integer" -> (int) value;
      default -> value;
    };
  }

  private static boolean isNull(long row) {
    return NULL_EVERY > 0 && row % NULL_EVERY == 0;
  }
}
