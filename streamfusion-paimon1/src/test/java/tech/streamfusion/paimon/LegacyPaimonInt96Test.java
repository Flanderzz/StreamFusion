package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.parquet.reader.TimestampColumnReader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Paimon 1.0's reader rejects the negative fraction its own INT96 writer emits. */
class LegacyPaimonInt96Test {
  @TempDir java.nio.file.Path directory;

  @Test
  void negativeFractionBytesAndReleasedReaderFailuresMatch() throws Exception {
    var type =
        new RowType(
            List.of(
                new DataField(0, "ts7", DataTypes.TIMESTAMP(7)),
                new DataField(1, "ts8", DataTypes.TIMESTAMP(8)),
                new DataField(2, "ts9", DataTypes.TIMESTAMP(9)),
                new DataField(3, "ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9))));
    var logicalType = LogicalTypeConversion.toLogicalType(type);
    var io = LocalFileIO.create();
    List<List<byte[]>> twins = new ArrayList<>();
    for (boolean nativeWriter : new boolean[] {false, true}) {
      var path =
          new Path(directory.resolve(nativeWriter ? "native.parquet" : "stock.parquet").toUri());
      var factory = FileFormat.fromIdentifier("parquet", new Options()).createWriterFactory(type);
      try (var out = io.newOutputStream(path, false);
          var allocator = new RootAllocator()) {
        var writer = assertInstanceOf(NativePaimonFileWriter.class, factory.create(out, "zstd"));
        if (nativeWriter) {
          try (var root =
              RowDataArrowConverter.write(
                  List.of(
                      GenericRowData.of(
                          TimestampData.fromEpochMillis(-1, 123400),
                              TimestampData.fromEpochMillis(-1, 123450),
                          TimestampData.fromEpochMillis(-1, 123456),
                              TimestampData.fromEpochMillis(-1, 123456))),
                  logicalType,
                  allocator)) {
            writer.writeBundle(new ArrowBatchBundle(root, logicalType));
          }
        } else {
          writer.addElement(
              GenericRow.of(
                  Timestamp.fromEpochMillis(-1, 123400),
                  Timestamp.fromEpochMillis(-1, 123450),
                  Timestamp.fromEpochMillis(-1, 123456),
                  Timestamp.fromEpochMillis(-1, 123456)));
        }
        writer.close();
        assertEquals(nativeWriter, writer.wroteNatively());
      }
      List<byte[]> columns = new ArrayList<>();
      try (var reader =
          ParquetReader.builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri()))
              .build()) {
        var row = reader.read();
        assertNotNull(row);
        for (String name : type.getFieldNames()) {
          byte[] bytes = row.getInt96(name, 0).getBytes();
          columns.add(bytes);
          var buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
          long nanos = buffer.getLong();
          int julian = buffer.getInt();
          assertThrows(
              IllegalArgumentException.class,
              () -> TimestampColumnReader.int96ToTimestamp(true, nanos, julian));
        }
        assertNull(reader.read());
      }
      twins.add(columns);
    }
    for (int i = 0; i < type.getFieldCount(); i++) {
      assertArrayEquals(twins.get(0).get(i), twins.get(1).get(i), type.getFieldNames().get(i));
    }
  }
}
