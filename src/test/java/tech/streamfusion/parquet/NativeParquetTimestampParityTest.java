package tech.streamfusion.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.RowDataArrowConverter;

class NativeParquetTimestampParityTest {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"millis", "micros", "nanos"})
  void physicalTimestampUnitsMatchFlinkIncludingOverflow(String unit) throws Exception {
    RowType type = RowType.of(new TimestampType(9));
    List<RowData> rows = new ArrayList<>();
    for (long millis : new long[] {
        Long.MIN_VALUE, -62_135_596_800_000L, -1, 0,
        9_223_459_200_000L, 253_402_300_799_999L, Long.MAX_VALUE
    }) {
      rows.add(GenericRowData.of(TimestampData.fromEpochMillis(millis, 999_999)));
    }
    rows.add(GenericRowData.of((Object) null));
    Path stock = directory.resolve("stock.parquet");
    Configuration config = new Configuration(false);
    config.setBoolean("parquet.write.int64.timestamp", true);
    config.set("parquet.timestamp.time.unit", unit);
    var path = new org.apache.flink.core.fs.Path(stock.toUri());
    try (var output = path.getFileSystem().create(path, WriteMode.NO_OVERWRITE)) {
      var writer = ParquetRowDataBuilder.createWriterFactory(type, config, true).create(output);
      for (RowData row : rows) writer.addElement(row);
      writer.finish();
    }
    Path nativeFile = directory.resolve("native.parquet");
    try (var allocator = new RootAllocator();
        var batch = RowDataArrowConverter.write(rows, type, allocator);
        var output = Files.newOutputStream(nativeFile);
        var writer = new ParquetCodec().createEncoder(batch.getSchema(), new int[0],
            new String[] {"timestamp.unit"}, new String[] {unit}, false, output)) {
      writer.write(batch, new int[0], 0, rows.size());
      writer.finish();
    }
    // Inspect the physical longs: a reader's timestamp conversion must not hide wire differences.
    assertEquals(physicalValues(stock), physicalValues(nativeFile));
  }

  private static List<Long> physicalValues(Path path) throws Exception {
    List<Long> values = new ArrayList<>();
    try (ParquetReader<Group> reader = ParquetReader.builder(
        new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri())).build()) {
      Group row;
      while ((row = reader.read()) != null) {
        values.add(row.getFieldRepetitionCount(0) == 0 ? null : row.getLong(0, 0));
      }
    }
    return values;
  }
}
