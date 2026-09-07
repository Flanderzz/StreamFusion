package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

/**
 * Streaming SQL inserts into Paimon append tables through the native sink, checked against twins
 * written by the stock Paimon connector in the same MiniCluster.
 */
class PaimonSinkParityTest {

  private static final int ROWS = 300;

  private static final String[][] FIXTURE_COLUMNS = {
    {"id", "BIGINT NOT NULL"},
    {"name", "STRING"},
    {"price", "DECIMAL(10, 2)"},
    {"big", "DECIMAL(20, 4)"},
    {"ts", "TIMESTAMP(3)"},
    {"ts6", "TIMESTAMP(6)"},
    {"dt", "DATE"},
    {"tags", "ARRAY<INT>"},
    {"attrs", "MAP<STRING, BIGINT>"},
    {"nested", "ROW<a INT, b STRING>"},
    {"flag", "BOOLEAN"},
    {"dbl", "DOUBLE"},
    {"bin", "BYTES"},
    {"pt", "STRING"}
  };

  private static final String COLUMNS =
      Stream.of(FIXTURE_COLUMNS)
          .map(column -> column[0] + " " + column[1])
          .collect(Collectors.joining(", "));

  @Test
  void fixedBucketPartitionedTableMatchesTheStockTwin() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-fixed");
    String options = "'bucket' = '2', 'bucket-key' = 'id,ts'";
    FileStoreTable nativeTable = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 1, true);
    FileStoreTable stockTable = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 1, false);

    assertSameTables(stockTable, nativeTable, true);
    assertBucketsMatchPaimonsExtractor(nativeTable);
  }

  /**
   * Past {@code write-max-writers-to-spill} writers in one task Paimon re-buffers what it has
   * written and spills through its stock row writer. Files stay identical apart from the sequence
   * numbers Paimon reassigns on that rewrite, which depend on how many rows each writer had already
   * taken: one row at a time on the stock path, one routed batch at a time on ours.
   */
  @Test
  void manyWritersInOneTaskSpillThroughTheStockWriterLikeStock() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-spill");
    String options = "'bucket' = '4', 'bucket-key' = 'id,ts'";
    FileStoreTable nativeTable = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 1, true);
    FileStoreTable stockTable = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 1, false);

    assertSameTables(stockTable, nativeTable, false);
    assertBucketsMatchPaimonsExtractor(nativeTable);
  }

  @Test
  void fixedBucketUnpartitionedTableShufflesLikeStockAcrossWriters() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-fixed-flat");
    String options = "'bucket' = '3', 'bucket-key' = 'name', 'file.compression' = 'snappy'";
    FileStoreTable nativeTable = insertFixture(warehouse, "", options, 2, true);
    FileStoreTable stockTable = insertFixture(warehouse, "", options, 2, false);

    assertSameTables(stockTable, nativeTable, true);
    assertBucketsMatchPaimonsExtractor(nativeTable);
  }

  @Test
  void unawareHashPartitionedTableMatchesTheStockTwin() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-unaware");
    String options = "'bucket' = '-1', 'partition.sink-strategy' = 'hash'";
    FileStoreTable nativeTable = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 2, true);
    FileStoreTable stockTable = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 2, false);

    assertSameTables(stockTable, nativeTable, true);
  }

  @Test
  void unawareTableCompactsInJobThroughTheStockRowWriter() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-compact");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(200);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    tableEnv.executeSql(
        "CREATE TABLE compacted (id BIGINT, name STRING, pt STRING) PARTITIONED BY (pt) WITH ("
            + "'bucket' = '-1', 'compaction.min.file-num' = '2', 'compaction.max.file-num' = '3',"
            + " 'continuous.discovery-interval' = '500 ms')");
    tableEnv.executeSql(
        "CREATE TEMPORARY TABLE ticks (id BIGINT, name STRING) WITH ('connector' = 'datagen',"
            + " 'rows-per-second' = '100', 'number-of-rows' = '600', 'fields.name.length' = '8')");
    PhysicalPlanScan scan = NativePlanner.install(tableEnv);

    tableEnv
        .executeSql("INSERT INTO compacted SELECT id, name, CAST(id % 3 AS STRING) FROM ticks")
        .await();

    assertAccelerated(scan);
    FileStoreTable table = openTable(warehouse, "compacted");
    assertEquals(600, PaimonTestTables.readRows(table, table.rowType()).size());
    List<Snapshot.CommitKind> kinds = new ArrayList<>();
    for (Iterator<Snapshot> snapshots = table.snapshotManager().snapshots(); snapshots.hasNext(); ) {
      kinds.add(snapshots.next().commitKind());
    }
    assertTrue(kinds.contains(Snapshot.CommitKind.COMPACT), kinds::toString);
    assertTrue(kinds.contains(Snapshot.CommitKind.APPEND), kinds::toString);
  }

  @Test
  void writerOptionsRefreshAtCheckpointsLikeTheStockOperator() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-refresh");
    java.nio.file.Path firstExternal = Files.createTempDirectory("paimon-sink-external-1");
    java.nio.file.Path secondExternal = Files.createTempDirectory("paimon-sink-external-2");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(200);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    tableEnv.executeSql(
        "CREATE TABLE relocated (id BIGINT, name STRING) WITH ('bucket' = '-1',"
            + " 'sink.writer-refresh-detectors' = 'external-paths',"
            + " 'data-file.external-paths' = '"
            + firstExternal.toUri()
            + "', 'data-file.external-paths.strategy' = 'round-robin')");
    tableEnv.executeSql(
        "CREATE TEMPORARY TABLE ticks (id BIGINT, name STRING) WITH ('connector' = 'datagen',"
            + " 'rows-per-second' = '50', 'number-of-rows' = '400', 'fields.name.length' = '8')");
    PhysicalPlanScan scan = NativePlanner.install(tableEnv);

    TableResult insert = tableEnv.executeSql("INSERT INTO relocated SELECT id, name FROM ticks");
    waitForFiles(firstExternal);
    tableEnv.executeSql(
        "ALTER TABLE relocated SET ('data-file.external-paths' = '" + secondExternal.toUri() + "')");
    insert.await();

    assertAccelerated(scan);
    FileStoreTable table = openTable(warehouse, "relocated");
    assertEquals(400, PaimonTestTables.readRows(table, table.rowType()).size());
    assertTrue(countFiles(secondExternal) >= 1, "no data file reached the refreshed external path");
  }

  private static void waitForFiles(java.nio.file.Path root) throws Exception {
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60);
    while (countFiles(root) == 0) {
      assertTrue(System.nanoTime() < deadline, "no data file reached " + root);
      Thread.sleep(100);
    }
  }

  private static long countFiles(java.nio.file.Path root) throws Exception {
    try (Stream<java.nio.file.Path> paths = Files.walk(root)) {
      return paths.filter(Files::isRegularFile).count();
    }
  }

  static Stream<Arguments> declinedTables() {
    return Stream.of(
        Arguments.of("(id BIGINT, v INT, PRIMARY KEY (id) NOT ENFORCED)", "'bucket' = '2'", "primary-key"),
        Arguments.of("(id BIGINT, v INT)", "'bucket' = '-1', 'file.format' = 'orc'", "file.format orc"),
        Arguments.of("(id BIGINT, v INT)", "'bucket' = '-1', 'write-buffer-for-append' = 'true'", "write-buffer-for-append"),
        Arguments.of("(id BIGINT, v INT)", "'bucket' = '-1', 'file-index.bloom-filter.columns' = 'v'", "file indexes"),
        Arguments.of("(id BIGINT, v INT)", "'bucket' = '-1', 'row-tracking.enabled' = 'true'", "row tracking"),
        Arguments.of("(id BIGINT, v INT)", "'bucket' = '-1', 'sink.clustering.by-columns' = 'v'", "clustering"),
        Arguments.of("(id BIGINT, v INT)", "'bucket' = '-1', 'sink.writer-coordinator.enabled' = 'true'", "sink.writer-coordinator.enabled"),
        Arguments.of("(id BIGINT, v INT)", "'bucket' = '-1', 'write-only' = 'true', 'sink.coordinator-commit.enabled' = 'true'", "sink.coordinator-commit.enabled"),
        Arguments.of("(id BIGINT, v INT, pt STRING) PARTITIONED BY (pt)", "'bucket' = '-1', 'partition.sink-strategy' = 'PARTITION_DYNAMIC'", "PARTITION_DYNAMIC"),
        Arguments.of("(id BIGINT, v TIMESTAMP(9))", "'bucket' = '-1'", "INT96"),
        Arguments.of("(id BIGINT, v INT)", "'bucket' = '-1', 'parquet.bloom.filter.enabled' = 'true'", "bloom"));
  }

  @ParameterizedTest
  @MethodSource("declinedTables")
  void tablesOutsideTheWhitelistFallBackWithAReason(String schema, String options, String reason)
      throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-declined");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(1000);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    tableEnv.executeSql("CREATE TABLE declined " + schema + " WITH (" + options + ")");
    tableEnv.executeSql(
        "CREATE TEMPORARY TABLE src (id BIGINT, v INT, pt STRING) WITH ('connector' = 'datagen')");
    PhysicalPlanScan scan = NativePlanner.install(tableEnv);

    tableEnv.explainSql("INSERT INTO declined SELECT * FROM (SELECT id, " + selectFor(schema) + " FROM src)");

    assertDeclined(scan, reason);
  }

  @Test
  void dynamicTableOptionsFromTheJobConfigurationShapeAdmission() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-dynamic");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    tableEnv.executeSql("CREATE TABLE dyn_opts (id BIGINT, v INT) WITH ('bucket' = '-1')");
    tableEnv.executeSql(
        "CREATE TEMPORARY TABLE src (id BIGINT, v INT) WITH ('connector' = 'datagen')");
    tableEnv.getConfig().set("paimon.*.*.dyn_opts.write-buffer-for-append", "true");
    PhysicalPlanScan scan = NativePlanner.install(tableEnv);

    tableEnv.explainSql("INSERT INTO dyn_opts SELECT * FROM src");

    assertDeclined(scan, "write-buffer-for-append");
  }

  private static String selectFor(String schema) {
    if (schema.contains("TIMESTAMP(9)")) {
      return "CAST(TO_TIMESTAMP_LTZ(id, 3) AS TIMESTAMP(9)) AS v";
    }
    return schema.contains("pt STRING") ? "v, pt" : "v";
  }

  private static FileStoreTable insertFixture(
      java.nio.file.Path warehouse,
      String partitioning,
      String options,
      int parallelism,
      boolean nativeSink)
      throws Exception {
    String name = nativeSink ? "fixture_native" : "fixture_stock";
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    tableEnv.executeSql(
        "CREATE TABLE " + name + " (" + COLUMNS + ") " + partitioning + " WITH (" + options + ")");
    DataStream<Row> stream = env.fromData(fixtureTypeInformation(), fixtureRows());
    Table source = tableEnv.fromDataStream(stream, fixtureSchema());
    tableEnv.createTemporaryView("fixture_source", source);
    PhysicalPlanScan scan = nativeSink ? NativePlanner.install(tableEnv) : null;

    tableEnv.executeSql("INSERT INTO " + name + " SELECT * FROM fixture_source").await();

    if (nativeSink) {
      assertAccelerated(scan);
    }
    return openTable(warehouse, name);
  }

  private static StreamTableEnvironment catalogEnvironment(
      StreamExecutionEnvironment env, java.nio.file.Path warehouse) {
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    tableEnv.executeSql(
        "CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse' = '" + warehouse.toUri() + "')");
    tableEnv.executeSql("USE CATALOG paimon");
    return tableEnv;
  }

  private static FileStoreTable openTable(java.nio.file.Path warehouse, String name) {
    Path path = new Path(warehouse.resolve("default.db").resolve(name).toUri());
    return FileStoreTableFactory.create(LocalFileIO.create(), path);
  }

  private static TypeInformation<Row> fixtureTypeInformation() {
    return Types.ROW_NAMED(
        Stream.of(FIXTURE_COLUMNS).map(column -> column[0]).toArray(String[]::new),
        Types.LONG,
        Types.STRING,
        Types.BIG_DEC,
        Types.BIG_DEC,
        Types.LOCAL_DATE_TIME,
        Types.LOCAL_DATE_TIME,
        Types.LOCAL_DATE,
        Types.OBJECT_ARRAY(Types.INT),
        Types.MAP(Types.STRING, Types.LONG),
        Types.ROW_NAMED(new String[] {"a", "b"}, Types.INT, Types.STRING),
        Types.BOOLEAN,
        Types.DOUBLE,
        Types.PRIMITIVE_ARRAY(Types.BYTE),
        Types.STRING);
  }

  private static Schema fixtureSchema() {
    Schema.Builder schema = Schema.newBuilder();
    for (String[] column : FIXTURE_COLUMNS) {
      schema.column(column[0], column[1]);
    }
    return schema.build();
  }

  private static Row[] fixtureRows() {
    List<Row> rows = new ArrayList<>();
    for (Object[] v : PaimonTestTables.values(ROWS)) {
      long[] micros = (long[]) v[5];
      Object[] nested = (Object[]) v[9];
      rows.add(
          Row.of(
              v[0],
              v[1],
              v[2],
              v[3],
              v[4] == null ? null : LocalDateTime.ofEpochSecond((Long) v[4] / 1000, (int) ((Long) v[4] % 1000) * 1_000_000, ZoneOffset.UTC),
              micros == null
                  ? null
                  : LocalDateTime.ofEpochSecond(micros[0] / 1000, (int) (micros[0] % 1000) * 1_000_000 + (int) micros[1], ZoneOffset.UTC),
              v[6] == null ? null : LocalDate.ofEpochDay((Integer) v[6]),
              v[7],
              v[8],
              nested == null ? null : Row.of(nested[0], nested[1]),
              v[10],
              v[11],
              v[12],
              v[13]));
    }
    return rows.toArray(new Row[0]);
  }

  private static void assertSameTables(
      FileStoreTable expected, FileStoreTable actual, boolean absoluteSequenceNumbers)
      throws Exception {
    RowType rowType = expected.rowType();
    List<String> expectedRows = PaimonTestTables.readRows(expected, rowType);
    assertEquals(ROWS, expectedRows.size());
    assertEquals(expectedRows, PaimonTestTables.readRows(actual, rowType));
    Map<String, List<DataFileMeta>> expectedFiles = PaimonTestTables.dataFiles(expected);
    Map<String, List<DataFileMeta>> actualFiles = PaimonTestTables.dataFiles(actual);
    assertEquals(expectedFiles.keySet(), actualFiles.keySet());
    for (String destination : expectedFiles.keySet()) {
      assertEquals(
          describeAll(expectedFiles.get(destination), rowType, absoluteSequenceNumbers),
          describeAll(actualFiles.get(destination), rowType, absoluteSequenceNumbers),
          destination);
    }
    assertEquals(PaimonTestTables.footers(expected), PaimonTestTables.footers(actual));
  }

  private static List<String> describeAll(
      List<DataFileMeta> files, RowType rowType, boolean absoluteSequenceNumbers) {
    return files.stream()
        .map(
            file ->
                "rows="
                    + file.rowCount()
                    + " seq="
                    + (absoluteSequenceNumbers ? file.minSequenceNumber() + ".." : "span ")
                    + (file.maxSequenceNumber() - (absoluteSequenceNumbers ? 0 : file.minSequenceNumber()))
                    + " level="
                    + file.level()
                    + " schema="
                    + file.schemaId()
                    + " format="
                    + file.fileFormat()
                    + " deletes="
                    + file.deleteRowCount()
                    + "\n"
                    + PaimonTestTables.describe(file, rowType))
        .collect(Collectors.toList());
  }

  /** Every row read from a (partition, bucket) split routes there under Paimon's own extractor. */
  private static void assertBucketsMatchPaimonsExtractor(FileStoreTable table) throws Exception {
    FixedBucketRowKeyExtractor extractor = new FixedBucketRowKeyExtractor(table.schema());
    RowType rowType = table.rowType();
    int checked = 0;
    for (Split split : table.newReadBuilder().newScan().plan().splits()) {
      DataSplit dataSplit = (DataSplit) split;
      List<InternalRow> rows = new ArrayList<>();
      table
          .newReadBuilder()
          .newRead()
          .createReader(dataSplit)
          .forEachRemaining(row -> rows.add(InternalRowUtils.copyInternalRow(row, rowType)));
      for (InternalRow row : rows) {
        extractor.setRecord(row);
        BinaryRow partition = extractor.partition();
        assertEquals(dataSplit.partition(), partition, () -> "partition of " + row);
        assertEquals(dataSplit.bucket(), extractor.bucket(), () -> "bucket of " + row);
        checked++;
      }
    }
    assertEquals(ROWS, checked);
  }

  private static void assertAccelerated(PhysicalPlanScan scan) {
    assertTrue(
        scan.substitutions() > 0,
        () -> "Paimon sink did not accelerate: " + scan.explainSummary());
  }

  private static void assertDeclined(PhysicalPlanScan scan, String reason) {
    List<String> reasons = scan.fallbackReasons();
    assertTrue(
        reasons.stream().anyMatch(r -> r.startsWith("paimon sink: ") && r.contains(reason)),
        () -> "expected a paimon sink decline mentioning '" + reason + "' in " + reasons);
  }
}
