package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.orc.NativeOrc;

/** File open through consumption in arrow-rs; never imports native batches into Java. */
@EnabledIfEnvironmentVariable(named = "SF_ORC_READER_COMPARISON", matches = "true")
class OrcReaderComparisonBenchmark {
  private static final String[] BACKENDS = {"nanoarrow", "arrow_cpp", "orc_rust", "java"};
  private static final int BATCH_ROWS = 4096;
  @TempDir java.nio.file.Path directory;

  private record Fixture(FileStoreTable table, ReadBuilder read, List<Split> splits, boolean pk) {}

  static Stream<Arguments> scalarTypes() {
    var timestamps =
        List.of(-2208988799877L, -1001L, -1000L, -1L, 0L, 1L, 1700000000123L).stream()
            .map(t -> org.apache.paimon.data.Timestamp.fromEpochMillis(t, 456000))
            .toList();
    return Stream.concat(
        PaimonValueTypesTest.values().filter(a -> a.get()[2].equals("orc")),
        Stream.of(
            Arguments.of(org.apache.paimon.types.DataTypes.TIMESTAMP(6), timestamps, "orc"),
            Arguments.of(
                org.apache.paimon.types.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6),
                timestamps,
                "orc")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("scalarTypes")
  void scalarCompatibility(
      org.apache.paimon.types.DataType valueType, List<?> values, String format) throws Exception {
    var type =
        new org.apache.paimon.types.RowType(
            List.of(
                new org.apache.paimon.types.DataField(
                    0, "id", org.apache.paimon.types.DataTypes.INT().notNull()),
                new org.apache.paimon.types.DataField(1, "value", valueType)));
    var path = new Path(Files.createTempDirectory(directory, "scalar").toUri());
    var io = org.apache.paimon.fs.local.LocalFileIO.create();
    new org.apache.paimon.schema.SchemaManager(io, path)
        .createTable(
            new org.apache.paimon.schema.Schema(
                type.getFields(),
                List.of(),
                List.of(),
                Map.of("file.format", format, "bucket", "-1"),
                null));
    var table = org.apache.paimon.table.FileStoreTableFactory.create(io, path);
    var builder = table.newStreamWriteBuilder().withCommitUser("scalar-comparison");
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      writer.write(org.apache.paimon.data.GenericRow.of(0, null));
      for (int i = 0; i < values.size(); i++)
        writer.write(org.apache.paimon.data.GenericRow.of(i + 1, values.get(i)));
      commit.commit(1, writer.prepareCommit(true, 1));
    }
    var read = table.newReadBuilder();
    var fixture = new Fixture(table, read, read.newStreamScan().plan().splits(), false);
    var expected = scan(fixture, 3, true);
    var paddedChars = new Stats();
    if (valueType instanceof org.apache.paimon.types.CharType charType) {
      // Both released alternatives expose ORC's padded CHAR bytes. Paimon Java strips padding.
      // Assert the exact known difference, rather than accepting any mismatch from these readers.
      List<RowData> padded = new ArrayList<>();
      padded.add(org.apache.flink.table.data.GenericRowData.of(0, null));
      for (int i = 0; i < values.size(); i++) {
        String text = values.get(i).toString();
        text += " ".repeat(charType.getLength() - text.codePointCount(0, text.length()));
        padded.add(
            org.apache.flink.table.data.GenericRowData.of(
                i + 1, org.apache.flink.table.data.StringData.fromString(text)));
      }
      consumeJava(padded, LogicalTypeConversion.toLogicalType(type), false, true, paddedChars);
      assertNotEquals(expected.digest, paddedChars.digest);
    }
    List<org.junit.jupiter.api.function.Executable> checks = new ArrayList<>();
    for (int backend = 0; backend < 3; backend++) {
      int candidate = backend;
      checks.add(
          () -> {
            var actual = scan(fixture, candidate, true);
            assertEquals(expected.rows, actual.rows);
            boolean padded =
                candidate != 0 && valueType instanceof org.apache.paimon.types.CharType;
            assertEquals(
                padded ? paddedChars.digest : expected.digest,
                actual.digest,
                BACKENDS[candidate] + " " + valueType);
          });
    }
    assertAll(checks);
  }

  private Fixture fixture(int rows, boolean pk, boolean allColumns, String compression)
      throws Exception {
    FileStoreTable table =
        pk
            ? PaimonMergeEngineTest.table(
                Map.of(
                    "changelog-producer",
                    "input",
                    "write-only",
                    "true",
                    "scan.mode",
                    "latest",
                    "file.format",
                    "orc",
                    "file.compression",
                    compression))
            : PaimonTestTables.createTable(
                Files.createTempDirectory(directory, "append"),
                Map.of("bucket", "-1", "file.format", "orc", "file.compression", compression));
    if (pk) {
      try (var initial =
          new PaimonMergeEngineTest.Writer(
              table, false, new PaimonChangelogSinkWriteTest.MemoryState(), 7)) {
        initial.write(PaimonMergeEngineTest.rows(1, false));
        initial.commit(1);
      }
    }
    var read = table.newReadBuilder();
    if (!allColumns) read.withProjection(pk ? new int[] {0, 3, 4, 7} : new int[] {0, 1, 2, 7});
    var scan = read.newStreamScan();
    if (pk) scan.plan();
    var builder = table.newStreamWriteBuilder().withCommitUser("orc-reader-comparison");
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      if (pk) {
        for (var row : PaimonMergeEngineTest.rows(rows, false)) writer.write(row);
      } else {
        for (var row : PaimonTestTables.values(rows)) writer.write(PaimonTestTables.paimonRow(row));
      }
      commit.commit(2, writer.prepareCommit(true, 2));
    }
    return new Fixture(table, read, scan.plan().splits(), pk);
  }

  @Test
  void readerParity() throws Exception {
    for (String compression : List.of("none", "zlib", "snappy", "lz4", "zstd")) {
      for (boolean pk : new boolean[] {false, true}) {
        var fixture = fixture(257, pk, true, compression);
        var expected = scan(fixture, 3, true);
        for (int backend = 0; backend < 3; backend++) {
          var actual = scan(fixture, backend, true);
          assertEquals(expected.rows, actual.rows, BACKENDS[backend]);
          assertEquals(expected.checksum, actual.checksum, BACKENDS[backend]);
          assertEquals(
              expected.digest, actual.digest, BACKENDS[backend] + " " + compression + " pk=" + pk);
        }
      }
    }
    assertEquals("", NativeOrc.liveNativeHandles());
  }

  @Test
  void compareReaders() throws Exception {
    int rows = Integer.parseInt(System.getenv().getOrDefault("SF_ORC_COMPARISON_ROWS", "262144"));
    int trials = Integer.parseInt(System.getenv().getOrDefault("SF_ORC_COMPARISON_TRIALS", "5"));
    assertTrue(trials > 0);
    for (String compression :
        System.getenv().getOrDefault("SF_ORC_COMPARISON_CODECS", "none,zstd").split(",")) {
      for (String shape : List.of("append_projected", "append_full", "changelog_projected")) {
        var fixture =
            fixture(rows, shape.startsWith("changelog"), shape.endsWith("full"), compression);
        // Full-value fingerprints are deliberately outside the measured scans.
        var expected = scan(fixture, 3, true);
        assertEquals(rows, expected.rows);
        for (int backend = 0; backend < 3; backend++) {
          var actual = scan(fixture, backend, true);
          assertEquals(expected.digest, actual.digest, BACKENDS[backend] + " " + shape);
          assertEquals(rows, actual.rows);
        }
        double[][] seconds = new double[4][trials];
        long[] peakBatch = new long[4];
        long[] bytesRead = new long[4];
        for (int run = 0; run < trials + 2; run++) {
          for (int offset = 0; offset < 4; offset++) {
            int backend = (run + offset) % 4;
            long start = System.nanoTime();
            var actual = scan(fixture, backend, false);
            double elapsed = (System.nanoTime() - start) / 1e9;
            assertEquals(expected.rows, actual.rows, BACKENDS[backend]);
            assertEquals(expected.checksum, actual.checksum, BACKENDS[backend]);
            if (run >= 2) seconds[backend][run - 2] = elapsed;
            peakBatch[backend] = Math.max(peakBatch[backend], actual.peakBatchBytes);
            bytesRead[backend] = actual.bytesRead;
          }
        }
        for (int backend = 0; backend < 4; backend++) {
          Arrays.sort(seconds[backend]);
          System.out.printf(
              "ORC_TO_ARROW_RS shape=%s codec=%s rows=%d backend=%s median_s=%.6f"
                  + " min_s=%.6f peak_batch_bytes=%d io_bytes=%d%n",
              shape,
              compression,
              rows,
              BACKENDS[backend],
              seconds[backend][trials / 2],
              seconds[backend][0],
              peakBatch[backend],
              bytesRead[backend]);
        }
      }
    }
    assertEquals("", NativeOrc.liveNativeHandles());
  }

  private static final class Stats {
    long rows, checksum, peakBatchBytes, digest, bytesRead;

    void add(long[] values) {
      rows += values[0];
      checksum += values[1];
      peakBatchBytes = Math.max(peakBatchBytes, values[2]);
      digest += values[3];
    }
  }

  private static Stats scan(Fixture fixture, int backend, boolean verify) throws Exception {
    Stats stats = new Stats();
    RowType type = LogicalTypeConversion.toLogicalType(fixture.read.readType());
    if (backend == 3) {
      stats.bytesRead = -1;
      var copy = new RowDataSerializer(type);
      List<RowData> pending = new ArrayList<>(BATCH_ROWS);
      for (var split : fixture.splits) {
        try (var reader = fixture.read.newRead().createReader(split)) {
          org.apache.paimon.reader.RecordReader.RecordIterator<org.apache.paimon.data.InternalRow>
              batch;
          while ((batch = reader.readBatch()) != null) {
            org.apache.paimon.data.InternalRow row;
            while ((row = batch.next()) != null) {
              pending.add(copy.copy(new FlinkRowData(row)));
              if (pending.size() == BATCH_ROWS) {
                consumeJava(pending, type, fixture.pk, verify, stats);
                pending.clear();
              }
            }
            batch.releaseBatch();
          }
        }
        if (!pending.isEmpty()) {
          consumeJava(pending, type, fixture.pk, verify, stats);
          pending.clear();
        }
      }
      return stats;
    }
    List<Field> fields = new ArrayList<>(ArrowConversion.toArrowSchema(type).getFields());
    List<String> names = new ArrayList<>(type.getFieldNames());
    if (fixture.pk) {
      fields.add(Field.nullable(RowDataArrowConverter.ROW_KIND_COLUMN, new ArrowType.Int(8, true)));
      names.add("_VALUE_KIND");
    }
    for (var raw : fixture.splits) {
      var split = (DataSplit) raw;
      for (var file : split.dataFiles()) {
        var path = new Path(file.externalPath().orElse(split.bucketPath() + "/" + file.fileName()));
        try (var input = new Input(fixture.table.fileIO().newInputStream(path));
            var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
          Data.exportSchema(
              NativeAllocator.SHARED, new Schema(fields), NativeAllocator.DICTIONARIES, schema);
          stats.add(
              NativeOrc.compareReaders(
                  backend,
                  input,
                  file.fileSize(),
                  schema.memoryAddress(),
                  names.toArray(String[]::new),
                  BATCH_ROWS,
                  verify));
          stats.bytesRead += input.bytesRead;
        }
      }
    }
    return stats;
  }

  private static void consumeJava(
      List<RowData> rows, RowType type, boolean pk, boolean verify, Stats stats) {
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED, pk);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      stats.add(
          NativeOrc.consumeComparisonBatch(array.memoryAddress(), schema.memoryAddress(), verify));
    }
  }

  /** Matches the production seekable FileIO callback and its bounded copy buffer. */
  public static final class Input implements AutoCloseable {
    private final SeekableInputStream stream;
    private final byte[] chunk = new byte[64 * 1024];
    long bytesRead;

    Input(SeekableInputStream stream) {
      this.stream = stream;
    }

    public void readFully(long position, ByteBuffer target) throws IOException {
      stream.seek(position);
      while (target.hasRemaining()) {
        int count = stream.read(chunk, 0, Math.min(chunk.length, target.remaining()));
        if (count < 0) throw new EOFException("Truncated ORC input");
        if (count == 0) throw new IOException("ORC input made no progress");
        target.put(chunk, 0, count);
        bytesRead += count;
      }
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }
}
