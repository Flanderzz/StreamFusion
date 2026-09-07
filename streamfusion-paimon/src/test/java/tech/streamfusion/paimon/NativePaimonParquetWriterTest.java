package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.shade.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Tables written through the native bundle writer against twins Paimon wrote row by row: the rows
 * read back, the manifest's per-file metadata and statistics, and the Parquet footers must agree.
 */
class NativePaimonParquetWriterTest {

  private static final int ROWS = 200;

  @ParameterizedTest
  @ValueSource(strings = {"unaware-zstd", "fixed-snappy", "unaware-uncompressed-v2"})
  void nativeTablesMatchStockTwins(String variant) throws Exception {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("file.format", "parquet");
    switch (variant) {
      case "unaware-zstd" -> options.put("bucket", "-1");
      case "fixed-snappy" -> {
        options.put("bucket", "3");
        options.put("bucket-key", "id");
        options.put("file.compression", "snappy");
      }
      case "unaware-uncompressed-v2" -> {
        options.put("bucket", "-1");
        options.put("file.compression", "none");
        options.put("parquet.writer.version", "PARQUET_2_0");
      }
      default -> throw new IllegalArgumentException(variant);
    }
    List<Object[]> values = PaimonTestTables.values(ROWS);
    FileStoreTable nativeTable =
        PaimonTestTables.createTable(Files.createTempDirectory("paimon-native"), options);
    FileStoreTable twinTable =
        PaimonTestTables.createTable(Files.createTempDirectory("paimon-twin"), options);

    writeNatively(nativeTable, values);
    writeStock(twinTable, values);

    RowType rowType = nativeTable.rowType();
    assertEquals(readRows(twinTable, rowType), readRows(nativeTable, rowType));
    assertEquals(ROWS, readRows(nativeTable, rowType).size());

    Map<String, List<DataFileMeta>> nativeFiles = dataFiles(nativeTable);
    Map<String, List<DataFileMeta>> twinFiles = dataFiles(twinTable);
    assertEquals(twinFiles.keySet(), nativeFiles.keySet(), "same partitions and buckets");
    for (String destination : twinFiles.keySet()) {
      List<DataFileMeta> expected = twinFiles.get(destination);
      List<DataFileMeta> actual = nativeFiles.get(destination);
      assertEquals(expected.size(), actual.size(), "files in " + destination);
      for (int i = 0; i < expected.size(); i++) {
        DataFileMeta twin = expected.get(i);
        DataFileMeta ours = actual.get(i);
        assertEquals(twin.rowCount(), ours.rowCount(), "row count in " + destination);
        assertEquals(twin.minSequenceNumber(), ours.minSequenceNumber(), destination);
        assertEquals(twin.maxSequenceNumber(), ours.maxSequenceNumber(), destination);
        assertEquals(
            describe(twin, rowType),
            describe(ours, rowType),
            "footer statistics in " + destination);
        assertEquals(twin.valueStatsCols(), ours.valueStatsCols(), destination);
        assertEquals(twin.schemaId(), ours.schemaId(), destination);
        assertEquals(twin.level(), ours.level(), destination);
        assertEquals(twin.fileFormat(), ours.fileFormat(), destination);
        assertEquals(twin.extraFiles(), ours.extraFiles(), destination);
        assertEquals(twin.deleteRowCount(), ours.deleteRowCount(), destination);
      }
    }
    assertEquals(footers(twinTable), footers(nativeTable));
  }

  @Test
  void discoveryResolvesParquetToTheNativeFormat() {
    Options options = new Options();
    FileFormat format = FileFormat.fromIdentifier("parquet", options);
    assertInstanceOf(NativePaimonParquetFormat.class, format);
    assertEquals("parquet", format.getFormatIdentifier());
  }

  @Test
  void rowFedFilesDelegateToTheStockWriterAndNeverMixWithBundles() throws Exception {
    java.nio.file.Path dir = Files.createTempDirectory("paimon-modes");
    FileFormat format = FileFormat.fromIdentifier("parquet", new Options());
    RowType rowType = PaimonTestTables.paimonSchema(Map.of()).rowType();
    FormatWriterFactory factory = format.createWriterFactory(rowType);
    List<Object[]> values = PaimonTestTables.values(5);
    LocalFileIO fileIO = LocalFileIO.create();

    Path rowFile = new Path(dir.toUri() + "/rows.parquet");
    try (PositionOutputStream out = fileIO.newOutputStream(rowFile, false)) {
      NativePaimonParquetWriter writer = (NativePaimonParquetWriter) factory.create(out, "zstd");
      for (Object[] row : values) {
        writer.addElement(PaimonTestTables.paimonRow(row));
      }
      try (BufferAllocator allocator = new RootAllocator();
          VectorSchemaRoot root =
              RowDataArrowConverter.write(
                  List.of(PaimonTestTables.flinkRow(values.get(0))),
                  PaimonTestTables.FLINK_TYPE,
                  allocator)) {
        assertThrows(
            IllegalStateException.class, () -> writer.writeBundle(new ArrowBatchBundle(root)));
      }
      writer.close();
      assertFalse(writer.wroteNatively());
    }
    assertEquals(5, ParquetUtil.getParquetReader(fileIO, rowFile, fileIO.getFileSize(rowFile), new Options())
        .getFooter().getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum());

    Path bundleFile = new Path(dir.toUri() + "/bundle.parquet");
    try (PositionOutputStream out = fileIO.newOutputStream(bundleFile, false);
        BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root =
            RowDataArrowConverter.write(
                values.stream().map(PaimonTestTables::flinkRow).collect(Collectors.toList()),
                PaimonTestTables.FLINK_TYPE,
                allocator)) {
      NativePaimonParquetWriter writer = (NativePaimonParquetWriter) factory.create(out, "zstd");
      writer.writeBundle(new ArrowBatchBundle(root));
      assertThrows(
          IllegalStateException.class,
          () -> writer.addElement(PaimonTestTables.paimonRow(values.get(0))));
      writer.close();
      assertTrue(writer.wroteNatively());
    }
    assertEquals(5, ParquetUtil.getParquetReader(fileIO, bundleFile, fileIO.getFileSize(bundleFile), new Options())
        .getFooter().getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum());
  }

  @Test
  void unsupportedTypesKeepTheStockWriterFactory() {
    FileFormat format = FileFormat.fromIdentifier("parquet", new Options());
    RowType int96 =
        RowType.of(
            new org.apache.paimon.types.DataType[] {org.apache.paimon.types.DataTypes.TIMESTAMP(9)},
            new String[] {"ts9"});
    assertFalse(format.createWriterFactory(int96) instanceof NativePaimonParquetWriterFactory);
    assertTrue(
        ((NativePaimonParquetFormat) format).nativeWriterFallbackReason(int96).contains("INT96"));
    assertEquals(
        null,
        ((NativePaimonParquetFormat) format)
            .nativeWriterFallbackReason(PaimonTestTables.paimonSchema(Map.of()).rowType()));
  }

  /** Dense statistics carry only the columns named by {@code valueStatsCols}. */
  private static String describe(DataFileMeta file, RowType rowType) {
    org.apache.paimon.stats.SimpleStats stats = file.valueStats();
    if (file.valueStatsCols() != null) {
      rowType = rowType.project(file.valueStatsCols());
    }
    return "min="
        + PaimonTestTables.render(stats.minValues(), rowType)
        + "\nmax="
        + PaimonTestTables.render(stats.maxValues(), rowType)
        + "\nnulls="
        + java.util.Arrays.toString(stats.nullCounts().toLongArray());
  }

  private static void writeNatively(FileStoreTable table, List<Object[]> values) throws Exception {
    StreamTableWrite write = table.newStreamWriteBuilder().withCommitUser("native").newWrite();
    StreamTableCommit commit = table.newStreamWriteBuilder().withCommitUser("native").newCommit();
    Map<String, List<Object[]>> destinations = new LinkedHashMap<>();
    Map<String, BinaryRow> partitions = new LinkedHashMap<>();
    Map<String, Integer> buckets = new LinkedHashMap<>();
    for (Object[] row : values) {
      InternalRow paimonRow = PaimonTestTables.paimonRow(row);
      BinaryRow partition = write.getPartition(paimonRow);
      int bucket = write.getBucket(paimonRow);
      String key = partition + "@" + bucket;
      destinations.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
      partitions.put(key, partition.copy());
      buckets.put(key, bucket);
    }
    try (BufferAllocator allocator = new RootAllocator()) {
      for (String key : destinations.keySet()) {
        List<RowData> rows =
            destinations.get(key).stream().map(PaimonTestTables::flinkRow).collect(Collectors.toList());
        try (VectorSchemaRoot root =
            RowDataArrowConverter.write(rows, PaimonTestTables.FLINK_TYPE, allocator)) {
          write.writeBundle(partitions.get(key), buckets.get(key), new ArrowBatchBundle(root));
        }
      }
    }
    List<CommitMessage> messages = write.prepareCommit(false, 1);
    commit.commit(1, messages);
    write.close();
    commit.close();
  }

  private static void writeStock(FileStoreTable table, List<Object[]> values) throws Exception {
    StreamTableWrite write = table.newStreamWriteBuilder().withCommitUser("twin").newWrite();
    StreamTableCommit commit = table.newStreamWriteBuilder().withCommitUser("twin").newCommit();
    for (Object[] row : values) {
      write.write(PaimonTestTables.paimonRow(row));
    }
    List<CommitMessage> messages = write.prepareCommit(false, 1);
    commit.commit(1, messages);
    write.close();
    commit.close();
  }

  private static List<String> readRows(FileStoreTable table, RowType rowType) throws Exception {
    List<String> rows = new ArrayList<>();
    table
        .newReadBuilder()
        .newRead()
        .createReader(table.newReadBuilder().newScan().plan())
        .forEachRemaining(row -> rows.add(PaimonTestTables.render(row, rowType)));
    rows.sort(String::compareTo);
    return rows;
  }

  private static Map<String, List<DataFileMeta>> dataFiles(FileStoreTable table) {
    Map<String, List<DataFileMeta>> files = new TreeMap<>();
    for (Split split : table.newReadBuilder().newScan().plan().splits()) {
      DataSplit dataSplit = (DataSplit) split;
      files
          .computeIfAbsent(dataSplit.partition() + "@" + dataSplit.bucket(), k -> new ArrayList<>())
          .addAll(dataSplit.dataFiles());
    }
    return files;
  }

  /** The schema, row-group row counts, and per-column codecs of every data file, by destination. */
  private static Map<String, List<String>> footers(FileStoreTable table) throws Exception {
    Map<String, List<String>> footers = new TreeMap<>();
    LocalFileIO fileIO = LocalFileIO.create();
    for (Split split : table.newReadBuilder().newScan().plan().splits()) {
      DataSplit dataSplit = (DataSplit) split;
      List<String> described = new ArrayList<>();
      for (DataFileMeta file : dataSplit.dataFiles()) {
        Path path = new Path(dataSplit.bucketPath(), file.fileName());
        try (ParquetFileReader reader =
            ParquetUtil.getParquetReader(fileIO, path, file.fileSize(), new Options())) {
          ParquetMetadata footer = reader.getFooter();
          StringBuilder description = new StringBuilder(footer.getFileMetaData().getSchema().toString());
          for (BlockMetaData block : footer.getBlocks()) {
            description.append("\nrows=").append(block.getRowCount());
            for (ColumnChunkMetaData column : block.getColumns()) {
              description.append(' ').append(column.getPath()).append(':').append(column.getCodec());
            }
          }
          described.add(description.toString());
        }
      }
      footers.put(dataSplit.partition() + "@" + dataSplit.bucket(), described);
    }
    return footers;
  }
}
