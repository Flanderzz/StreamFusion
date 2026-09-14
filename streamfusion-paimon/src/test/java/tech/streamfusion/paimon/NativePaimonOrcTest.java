package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.source.DataSplit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.orc.NativeOrc;

/** Java/native cross-reading, footer statistics and C Data ownership for every ORC codec. */
class NativePaimonOrcTest {
  static java.util.stream.Stream<Map<String, String>> writerOptions() {
    return java.util.stream.Stream.of(
        Map.of(
            "orc.compress",
            "ZSTD",
            "orc.compression.zstd.level",
            "9",
            "orc.compression.strategy",
            "SPEED"),
        Map.of(
            "orc.row.index.stride",
            "1000",
            "orc.compress.size",
            "4096",
            "orc.stripe.size",
            "16384",
            "orc.dictionary.key.threshold",
            "0"),
        Map.of("orc.create.index", "false", "orc.write.format", "0.11"),
        Map.of("orc.row.index.stride", "0"),
        Map.of(
            "orc.bloom.filter.columns",
            "id,name",
            "orc.bloom.filter.fpp",
            "0.01",
            "file.compression.zstd-level",
            "3",
            "orc.compression.strategy",
            "COMPRESSION"));
  }

  @ParameterizedTest
  @MethodSource("writerOptions")
  void configuredFilesMatchJava(Map<String, String> settings) throws Exception {
    var options = new java.util.HashMap<>(settings);
    options.put("file.format", "orc");
    options.put("bucket", "-1");
    var ours =
        PaimonTestTables.createTable(Files.createTempDirectory("orc-options-native"), options);
    var stock =
        PaimonTestTables.createTable(Files.createTempDirectory("orc-options-stock"), options);
    assertNull(
        ((NativePaimonOrcFormat)
                org.apache.paimon.format.FileFormat.fromIdentifier(
                    "orc", new org.apache.paimon.options.Options(options)))
            .nativeWriterFallbackReason(ours.rowType()));
    var values = PaimonTestTables.values(2000);
    NativePaimonParquetWriterTest.writeNatively(ours, values);
    NativePaimonParquetWriterTest.writeStock(stock, values);
    NativeAppendSinkWriteTest.assertNativeFiles(ours);
    assertEquals(
        PaimonTestTables.readRows(stock, stock.rowType()),
        PaimonTestTables.readRows(ours, ours.rowType()));
    List<String> expected = null;
    for (var table : List.of(stock, ours)) {
      List<String> actual = new ArrayList<>();
      for (var raw : table.newReadBuilder().newScan().plan().splits()) {
        var split = (DataSplit) raw;
        for (var file : split.dataFiles()) {
          try (var reader =
                  org.apache.paimon.format.orc.OrcReaderFactory.createReader(
                      new org.apache.hadoop.conf.Configuration(false),
                      table.fileIO(),
                      new Path(split.bucketPath(), file.fileName()),
                      null);
              var rows = reader.rows(new org.apache.paimon.shade.org.apache.orc.Reader.Options())) {
            var bloomColumns = new java.util.TreeSet<Integer>();
            for (var stripe : reader.getStripes()) {
              var footer =
                  ((org.apache.paimon.shade.org.apache.orc.impl.RecordReaderImpl) rows)
                      .readStripeFooter(stripe);
              for (var stream : footer.getStreamsList())
                if (stream.getKind().name().startsWith("BLOOM_FILTER"))
                  bloomColumns.add(stream.getColumn());
            }
            if (settings.containsKey("orc.bloom.filter.columns")) {
              assertEquals(
                  java.util.Set.of(
                      reader.getSchema().findSubtype("id").getId(),
                      reader.getSchema().findSubtype("name").getId()),
                  bloomColumns);
            }
            if (settings.containsKey("orc.compress.size"))
              assertTrue(reader.getCompressionSize() > 0 && reader.getCompressionSize() <= 4096);
            actual.add(
                reader.getRowIndexStride()
                    + ":"
                    + reader.getFileVersion()
                    + ":"
                    + reader.getCompressionKind()
                    + ":"
                    + bloomColumns);
          }
        }
      }
      actual.sort(String::compareTo);
      if (expected == null) expected = actual;
      else assertEquals(expected, actual);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"none", "zlib", "snappy", "lz4", "zstd"})
  void nativeFilesAndArrowReadsMatchJava(String compression) throws Exception {
    var options = Map.of("file.format", "orc", "file.compression", compression, "bucket", "-1");
    var nativeTable =
        PaimonTestTables.createTable(Files.createTempDirectory("native-orc"), options);
    var stockTable = PaimonTestTables.createTable(Files.createTempDirectory("stock-orc"), options);
    var format =
        (NativePaimonOrcFormat)
            org.apache.paimon.format.FileFormat.fromIdentifier(
                "orc", new org.apache.paimon.options.Options(options));
    assertNull(format.nativeWriterFallbackReason(nativeTable.rowType()));
    var values = PaimonTestTables.values(200);
    NativePaimonParquetWriterTest.writeNatively(nativeTable, values);
    NativePaimonParquetWriterTest.writeStock(stockTable, values);
    var expected = PaimonTestTables.readRows(stockTable, stockTable.rowType());
    assertEquals(expected, PaimonTestTables.readRows(nativeTable, nativeTable.rowType()));
    var ours = PaimonTestTables.dataFiles(nativeTable);
    var stock = PaimonTestTables.dataFiles(stockTable);
    assertEquals(stock.keySet(), ours.keySet());
    for (var key : stock.keySet()) {
      assertEquals(stock.get(key).size(), ours.get(key).size());
      for (int i = 0; i < stock.get(key).size(); i++) {
        assertEquals(
            PaimonTestTables.describe(stock.get(key).get(i), stockTable.rowType()),
            PaimonTestTables.describe(ours.get(key).get(i), nativeTable.rowType()));
      }
    }
    for (var table : List.of(nativeTable, stockTable)) {
      List<String> decoded = new ArrayList<>();
      for (var raw : table.newReadBuilder().newScan().plan().splits()) {
        var split = (DataSplit) raw;
        for (var file : split.dataFiles()) {
          try (var reader =
              new NativePaimonFileReader(
                  PaimonCodecs.reader("orc"),
                  table.fileIO(),
                  new Path(split.bucketPath() + "/" + file.fileName()),
                  file.fileSize(),
                  ArrowConversion.toArrowSchema(PaimonTestTables.FLINK_TYPE),
                  table.rowType().getFieldNames().toArray(String[]::new),
                  7)) {
            assertTrue(reader.readerMemory() > 0);
            while (true) {
              try (var batch = reader.next()) {
                if (batch == null) break;
                for (var row : new ArrowBatchBundle(batch, PaimonTestTables.FLINK_TYPE))
                  decoded.add(PaimonTestTables.render(row, table.rowType()));
              }
            }
          }
        }
      }
      decoded.sort(String::compareTo);
      assertEquals(expected, decoded);
    }
    assertEquals("", NativeOrc.liveNativeHandles());
  }
}
