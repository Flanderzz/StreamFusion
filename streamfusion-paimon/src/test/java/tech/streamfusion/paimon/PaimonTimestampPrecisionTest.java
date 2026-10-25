package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class PaimonTimestampPrecisionTest {
  private static final RowType TYPE = timestampType();

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  void highPrecisionSinksRetainNativeWriters(String format) {
    var type =
        new RowType(
            List.of(
                new DataField(0, "ts", DataTypes.TIMESTAMP(9)),
                new DataField(1, "ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9))));
    FileFormat fileFormat = FileFormat.fromIdentifier(format, new Options());
    assertNull(
        fileFormat instanceof NativePaimonParquetFormat
            ? ((NativePaimonParquetFormat) fileFormat).nativeWriterFallbackReason(type)
            : ((NativePaimonOrcFormat) fileFormat).nativeWriterFallbackReason(type));
    if (format.equals("parquet")) {
      assertInstanceOf(
          NativePaimonParquetWriterFactory.class, fileFormat.createWriterFactory(type));
    }
    assertNotNull(PaimonArrowFields.unsupportedTypeReason(type), "source admission stays separate");
  }

  @ParameterizedTest
  @CsvSource({
    "parquet,deduplicate,true",
    "orc,deduplicate,true",
    "orc,deduplicate,false",
    "parquet,first-row,true",
    "orc,first-row,true",
    "parquet,partial-update,true",
    "orc,partial-update,true",
    "parquet,aggregation,true",
    "orc,aggregation,true"
  })
  void mixedPrecisionsMatchReleasedFilesAcrossCommitsAndRestart(
      String format, String mode, boolean legacy) throws Exception {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("file.format", format);
    options.put("merge-engine", mode);
    options.put("write-only", "true");
    if (format.equals("orc"))
      options.put("orc.timestamp-ltz.legacy.type", Boolean.toString(legacy));
    if (mode.equals("aggregation")) {
      for (var field : TYPE.getFields()) {
        if (field.type() instanceof TimestampType
            || field.type() instanceof LocalZonedTimestampType) {
          options.put("fields." + field.name() + ".aggregate-function", "min");
        }
      }
    }
    var stock = PaimonMergeEngineTest.table(options, TYPE);
    var ours = PaimonMergeEngineTest.table(options, TYPE);
    var stockState = new PaimonChangelogSinkWriteTest.MemoryState();
    var nativeState = new PaimonChangelogSinkWriteTest.MemoryState();
    for (int pass = 0; pass < 2; pass++) {
      try (var left = new PaimonMergeEngineTest.Writer(stock, false, stockState, 3);
          var right = new PaimonMergeEngineTest.Writer(ours, true, nativeState, 3)) {
        var input = timestampRows(pass);
        left.write(input);
        right.write(input);
        left.commit(pass + 1);
        var messages = right.commit(pass + 1);
        int files = 0;
        for (var message : messages) {
          var data = (CommitMessageImpl) message;
          var paths =
              ours.store().pathFactory().createDataFilePathFactory(data.partition(), data.bucket());
          for (var file : data.newFilesIncrement().newFiles()) {
            PaimonTimestampSqlTest.assertNativeFile(ours, file, paths.toPath(file));
            files++;
          }
        }
        assertTrue(files > 0, "each checkpoint must contain native-written timestamp files");
      }
      // Released first-row batch scans hide level-0 files until compaction.
      if (mode.equals("first-row")) {
        compact(stock, pass + 10);
        compact(ours, pass + 10);
      }
      assertEquals(PaimonTestTables.readRows(stock, TYPE), PaimonTestTables.readRows(ours, TYPE));
      assertEquals(timestampRows(pass).size(), PaimonTestTables.readRows(ours, TYPE).size());
    }
    var expected = PaimonTestTables.dataFiles(stock);
    var actual = PaimonTestTables.dataFiles(ours);
    assertEquals(expected.keySet(), actual.keySet());
    for (var key : expected.keySet()) {
      assertEquals(expected.get(key).size(), actual.get(key).size());
      for (int i = 0; i < expected.get(key).size(); i++) {
        assertEquals(
            PaimonTestTables.describe(expected.get(key).get(i), TYPE),
            PaimonTestTables.describe(actual.get(key).get(i), TYPE),
            "manifest statistics " + key);
      }
    }
    if (format.equals("parquet")) {
      assertEquals(PaimonTestTables.footers(stock), PaimonTestTables.footers(ours));
    }
    compact(stock, 20);
    compact(ours, 20);
    assertEquals(PaimonTestTables.readRows(stock, TYPE), PaimonTestTables.readRows(ours, TYPE));
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  void nanosecondKeysAndSequencesMatchTheReleasedWriter(String format) throws Exception {
    var type =
        new RowType(
            List.of(
                new DataField(0, "id", DataTypes.TIMESTAMP(9).notNull()),
                new DataField(1, "v", DataTypes.INT()),
                new DataField(2, "seq", DataTypes.TIMESTAMP(9))));
    var options = Map.of("file.format", format, "sequence.field", "seq", "write-only", "true");
    var stock = PaimonMergeEngineTest.table(options, type);
    var ours = PaimonMergeEngineTest.table(options, type);
    var stockState = new PaimonChangelogSinkWriteTest.MemoryState();
    var nativeState = new PaimonChangelogSinkWriteTest.MemoryState();
    for (int pass = 0; pass < 2; pass++) {
      List<InternalRow> rows = new ArrayList<>();
      for (int nano : new int[] {0, 1, 2, 999999}) {
        var key = Timestamp.fromEpochMillis(-1, nano);
        rows.add(GenericRow.of(key, pass, Timestamp.fromEpochMillis(-1, pass == 0 ? 2 : 1)));
        rows.add(GenericRow.of(key, 10 + pass, Timestamp.fromEpochMillis(-1, pass == 0 ? 1 : 3)));
      }
      try (var left = new PaimonMergeEngineTest.Writer(stock, false, stockState, 3);
          var right = new PaimonMergeEngineTest.Writer(ours, true, nativeState, 3)) {
        left.write(rows);
        right.write(rows);
        left.commit(pass + 1);
        right.commit(pass + 1);
      }
      NativeAppendSinkWriteTest.assertNativeFiles(ours);
      var actual = PaimonTestTables.readRows(ours, type);
      assertEquals(4, actual.size(), "nanosecond-only key differences must remain distinct");
      assertEquals(PaimonTestTables.readRows(stock, type), actual);
    }
    compact(stock, 20);
    compact(ours, 20);
    assertEquals(PaimonTestTables.readRows(stock, type), PaimonTestTables.readRows(ours, type));
  }

  @ParameterizedTest
  @ValueSource(strings = {"America/New_York", "Europe/Berlin", "Asia/Kolkata"})
  void highPrecisionDoesNotBroadenOrcTimezoneAdmission(String zone) {
    TimeZone previous = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      var format = (NativePaimonOrcFormat) FileFormat.fromIdentifier("orc", new Options());
      assertTrue(format.nativeWriterFallbackReason(TYPE).contains("UTC"));
    } finally {
      TimeZone.setDefault(previous);
    }
  }

  private static void compact(FileStoreTable table, long checkpoint) throws Exception {
    var builder =
        table
            .copy(Map.of("write-only", "false"))
            .newStreamWriteBuilder()
            .withCommitUser("timestamp-compaction");
    try (var disk =
            org.apache.paimon.disk.IOManager.create(
                Files.createTempDirectory("paimon-timestamp-compaction").toString());
        var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      writer.withIOManager(disk);
      for (int bucket = 0; bucket < 2; bucket++) writer.compact(BinaryRow.EMPTY_ROW, bucket, true);
      commit.commit(checkpoint, writer.prepareCommit(true, checkpoint));
    }
  }

  private static RowType timestampType() {
    List<DataField> fields = new ArrayList<>();
    fields.add(new DataField(0, "id", DataTypes.INT().notNull()));
    for (int precision : new int[] {0, 3, 6, 7, 8, 9}) {
      fields.add(new DataField(fields.size(), "ts" + precision, DataTypes.TIMESTAMP(precision)));
      fields.add(
          new DataField(
              fields.size(),
              "ltz" + precision,
              DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(precision)));
    }
    fields.add(
        new DataField(
            20,
            "nested",
            DataTypes.ROW(
                DataTypes.FIELD(21, "ts", DataTypes.TIMESTAMP(9)),
                DataTypes.FIELD(22, "ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(8)))));
    fields.add(new DataField(23, "arr", DataTypes.ARRAY(DataTypes.TIMESTAMP(7))));
    fields.add(new DataField(24, "map", DataTypes.MAP(DataTypes.STRING(), DataTypes.TIMESTAMP(9))));
    fields.add(
        new DataField(25, "time_keys", DataTypes.MAP(DataTypes.TIMESTAMP(9), DataTypes.INT())));
    return new RowType(fields);
  }

  private static List<InternalRow> timestampRows(int pass) {
    long[] times = {
      -62_135_596_800_000L,
      -12_219_292_800_001L,
      -86_400_001L,
      -86_400_000L,
      -1001,
      -1000,
      -1,
      0,
      1,
      9_223_459_200_000L,
      253_402_300_799_999L
    };
    List<InternalRow> rows = new ArrayList<>();
    for (int i = 0; i <= times.length; i++) {
      GenericRow row = new GenericRow(TYPE.getFieldCount());
      row.setField(0, i);
      if (i < times.length) {
        long millis = times[pass == 0 ? i : times.length - 1 - i];
        int nanos = pass == 0 ? 999999 : 123456;
        for (int c = 1; c <= 12; c++) {
          var type = TYPE.getTypeAt(c);
          int precision =
              type instanceof TimestampType
                  ? ((TimestampType) type).getPrecision()
                  : ((LocalZonedTimestampType) type).getPrecision();
          row.setField(c, timestamp(millis, nanos, precision));
        }
        row.setField(13, GenericRow.of(timestamp(millis, nanos, 9), timestamp(millis, nanos, 8)));
        row.setField(14, new GenericArray(new Timestamp[] {null, timestamp(millis, nanos, 7)}));
        Map<BinaryString, Timestamp> map = new LinkedHashMap<>();
        map.put(BinaryString.fromString("value"), timestamp(millis, nanos, 9));
        map.put(BinaryString.fromString("null"), null);
        row.setField(15, new GenericMap(map));
        Map<Timestamp, Integer> timeKeys = new LinkedHashMap<>();
        timeKeys.put(timestamp(millis, nanos, 9), i);
        timeKeys.put(timestamp(millis, nanos - 1, 9), null);
        row.setField(16, new GenericMap(timeKeys));
        if (i == 8) {
          row.setField(14, new GenericArray(new Timestamp[0]));
          row.setField(15, new GenericMap(Map.of()));
          row.setField(16, new GenericMap(Map.of()));
        }
      }
      rows.add(row);
    }
    return rows;
  }

  private static Timestamp timestamp(long millis, int nanos, int precision) {
    long millisUnit = precision < 3 ? (long) Math.pow(10, 3 - precision) : 1;
    int nanoUnit = precision < 3 ? 1_000_000 : (int) Math.pow(10, 9 - precision);
    return Timestamp.fromEpochMillis(
        Math.floorDiv(millis, millisUnit) * millisUnit, nanos / nanoUnit * nanoUnit);
  }
}
