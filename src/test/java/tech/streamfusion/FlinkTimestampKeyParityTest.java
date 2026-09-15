package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class FlinkTimestampKeyParityTest {
  @ParameterizedTest
  @EnumSource(TimeUnit.class)
  void everyArrowUnitMatchesFlinksScalarAndNestedKeyBytes(TimeUnit unit) {
    long[] values =
        unit == TimeUnit.SECOND
            ? new long[] {
              -62_135_596_800L,
              -1,
              0,
              253_402_300_799L,
              Long.MIN_VALUE / 1000,
              Long.MAX_VALUE / 1000
            }
            : new long[] {Long.MIN_VALUE, -1_000_001, -1, 0, 1, Long.MAX_VALUE};
    for (int precision : new int[] {0, 3, 6, 9}) {
      for (boolean nested : new boolean[] {false, true}) {
        Field timestamp =
            new Field("ts", FieldType.nullable(new ArrowType.Timestamp(unit, "UTC")), List.of());
        Field field =
            nested
                ? new Field("k", FieldType.nullable(ArrowType.List.INSTANCE), List.of(timestamp))
                : timestamp;
        TimestampType logicalTimestamp = new TimestampType(precision);
        RowType rowType = RowType.of(nested ? new ArrayType(logicalTimestamp) : logicalTimestamp);
        RowDataSerializer serializer = new RowDataSerializer(rowType);
        int rows = nested ? 2 : values.length + 1;
        try (BufferAllocator allocator = new RootAllocator();
            VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(field)), allocator);
            CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
          root.allocateNew();
          ListVector list = nested ? (ListVector) root.getVector(0) : null;
          TimeStampVector timestamps =
              (TimeStampVector) (nested ? list.getDataVector() : root.getVector(0));
          TimestampData[] expectedValues = new TimestampData[values.length + 1];
          if (nested) {
            list.startNewValue(0);
          }
          for (int i = 0; i < values.length; i++) {
            timestamps.setSafe(i, values[i]);
            expectedValues[i] = reference(values[i], unit);
          }
          timestamps.setNull(values.length);
          if (nested) {
            list.endValue(0, expectedValues.length);
            list.setNull(1);
          }
          root.setRowCount(rows);
          Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
          byte[][] actual =
              Native.flinkBinaryRows(
                  array.memoryAddress(),
                  schema.memoryAddress(),
                  new int[] {0},
                  nested ? new int[] {-1, precision} : new int[] {precision},
                  IntStream.range(0, rows).toArray());
          for (int i = 0; i < rows; i++) {
            Object key =
                nested ? (i == 0 ? new GenericArrayData(expectedValues) : null) : expectedValues[i];
            BinaryRowData binary = serializer.toBinaryRow(GenericRowData.of(key));
            byte[] expected = new byte[binary.getSizeInBytes()];
            binary.getSegments()[0].get(binary.getOffset(), expected, 0, expected.length);
            assertArrayEquals(
                expected,
                actual[i],
                unit + " precision=" + precision + " nested=" + nested + " row=" + i);
          }
        }
      }
    }
  }

  private static TimestampData reference(long raw, TimeUnit unit) {
    long perSecond =
        switch (unit) {
          case SECOND -> 1;
          case MILLISECOND -> 1000;
          case MICROSECOND -> 1_000_000;
          case NANOSECOND -> 1_000_000_000;
        };
    return TimestampData.fromInstant(
        Instant.ofEpochSecond(raw / perSecond, raw % perSecond * (1_000_000_000 / perSecond)));
  }
}
