package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

/** Runtime SQL sources, committed native files, and released Paimon readers on both Flink lines. */
class PaimonTimestampSqlTest {
  @TempDir java.nio.file.Path tempDir;

  @ParameterizedTest
  @CsvSource({"parquet,false", "parquet,true", "orc,false", "orc,true"})
  void partitionedHighPrecisionSinksMatchStock(String format, boolean primaryKey) throws Exception {
    List<List<String>> twins = new ArrayList<>();
    for (boolean nativeSink : new boolean[] {false, true}) {
      var warehouse = Files.createTempDirectory(tempDir, "timestamps");
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      var sql = StreamTableEnvironment.create(env);
      sql.executeSql(
          "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='" + warehouse.toUri() + "')");
      sql.executeSql("USE CATALOG p");
      sql.executeSql(
          "CREATE TABLE t (id INT NOT NULL, ts7 TIMESTAMP(7), ts8 TIMESTAMP(8),"
              + " ts9 TIMESTAMP(9), ltz TIMESTAMP_LTZ(9), pt STRING NOT NULL"
              + (primaryKey ? ", PRIMARY KEY(id, pt) NOT ENFORCED" : "")
              + ") PARTITIONED BY(pt) WITH ('bucket'='"
              + (primaryKey ? 2 : -1)
              + "', 'write-only'='true', 'file.format'='"
              + format
              + "')");
      List<Row> inputRows = new ArrayList<>();
      for (int year : new int[] {1, 1582, 1969, 1970, 2026, 2263, 9999}) {
        // Paimon 1.0 rejects negative INT96 sub-millisecond remainders even in its own files.
        // The legacy file test covers that released failure; Paimon 2.0 covers exact fractions.
        var time =
            LocalDateTime.of(
                year,
                10,
                10,
                3,
                4,
                5,
                year < 1970 && format.equals("parquet") ? 123000000 : 123456789);
        inputRows.add(
            Row.of(
                year,
                time.withNano(time.getNano() / 100 * 100),
                time.withNano(time.getNano() / 10 * 10),
                time,
                time.toInstant(ZoneOffset.UTC),
                "p" + year % 2));
      }
      inputRows.add(Row.of(0, null, null, null, null, "nulls"));
      var input =
          env.fromCollection(
              inputRows,
              Types.ROW_NAMED(
                  new String[] {"id", "ts7", "ts8", "ts9", "ltz", "pt"},
                  Types.INT,
                  Types.LOCAL_DATE_TIME,
                  Types.LOCAL_DATE_TIME,
                  Types.LOCAL_DATE_TIME,
                  Types.INSTANT,
                  Types.STRING));
      sql.createTemporaryView(
          "input_rows",
          sql.fromDataStream(
              input,
              Schema.newBuilder()
                  .column("id", DataTypes.INT().notNull())
                  .column("ts7", DataTypes.TIMESTAMP(7))
                  .column("ts8", DataTypes.TIMESTAMP(8))
                  .column("ts9", DataTypes.TIMESTAMP(9))
                  .column("ltz", DataTypes.TIMESTAMP_LTZ(9))
                  .column("pt", DataTypes.STRING().notNull())
                  .build()));
      var plan = nativeSink ? NativePlanner.install(sql) : null;
      if (nativeSink) {
        assertTrue(
            sql.explainSql("INSERT INTO t SELECT * FROM input_rows").contains("NativePaimonSink"),
            plan.explainSummary());
      }
      sql.executeSql("INSERT INTO t SELECT * FROM input_rows").await();
      var table =
          FileStoreTableFactory.create(
              LocalFileIO.create(), new Path(warehouse.resolve("default.db/t").toUri()));
      if (nativeSink) {
        assertTrue(plan.substitutions() > 0, plan.explainSummary());
        assertNativeFiles(table);
        assertNotNull(NativePaimonSource.unsupportedTypeReason(table));
      }
      List<String> rows = new ArrayList<>();
      try (var reader =
          table.newReadBuilder().newRead().createReader(table.newReadBuilder().newScan().plan())) {
        reader
            .toCloseableIterator()
            .forEachRemaining(
                row ->
                    rows.add(
                        row.getInt(0)
                            + "|"
                            + (row.isNullAt(1) ? "null" : row.getTimestamp(1, 7))
                            + "|"
                            + (row.isNullAt(2) ? "null" : row.getTimestamp(2, 8))
                            + "|"
                            + (row.isNullAt(3) ? "null" : row.getTimestamp(3, 9))
                            + "|"
                            + (row.isNullAt(4) ? "null" : row.getTimestamp(4, 9))
                            + "|"
                            + row.getString(5)));
      }
      rows.sort(String::compareTo);
      assertEquals(inputRows.size(), rows.size());
      twins.add(rows);
    }
    assertEquals(twins.get(0), twins.get(1));
  }

  private static void assertNativeFiles(FileStoreTable table) throws Exception {
    int files = 0;
    for (var split : table.newReadBuilder().newScan().plan().splits()) {
      var data = (DataSplit) split;
      for (var file : data.dataFiles()) {
        var path = new Path(data.bucketPath(), file.fileName());
        assertNativeFile(table, file, path);
        files++;
      }
    }
    assertTrue(files > 0, "the SQL job must commit files written from Arrow columns");
  }

  static void assertNativeFile(FileStoreTable table, DataFileMeta file, Path path)
      throws Exception {
    if (file.fileFormat().equals("parquet")) {
      try (var reader =
          ParquetFileReader.open(
              HadoopInputFile.fromPath(
                  new org.apache.hadoop.fs.Path(path.toUri()),
                  new org.apache.hadoop.conf.Configuration()))) {
        assertTrue(reader.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"));
      }
    } else {
      try (var reader =
          org.apache.paimon.shade.org.apache.orc.OrcFile.createReader(
              new org.apache.hadoop.fs.Path(path.toUri()),
              org.apache.paimon.shade.org.apache.orc.OrcFile.readerOptions(
                  new org.apache.hadoop.conf.Configuration()))) {
        assertTrue(
            reader.hasMetadataValue(tech.streamfusion.orc.OrcVectorWriter.ARROW_WRITER_METADATA));
      }
    }
  }
}
