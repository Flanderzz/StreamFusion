package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import tech.streamfusion.arrow.vectors.ArrowTimestampColumnVector;

class TimestampAccessorTest {
  @ParameterizedTest
  @EnumSource(TimeUnit.class)
  void readsFlinkValueWithoutNarrowingToNanoseconds(TimeUnit unit) {
    Field field =
        new Field("ts", FieldType.nullable(new ArrowType.Timestamp(unit, "UTC")), List.of());
    long perSecond =
        switch (unit) {
          case SECOND -> 1;
          case MILLISECOND -> 1000;
          case MICROSECOND -> 1_000_000;
          case NANOSECOND -> 1_000_000_000;
        };
    long[] values =
        unit == TimeUnit.SECOND
            ? new long[] {Long.MIN_VALUE / 1000, -1, 0, 1, Long.MAX_VALUE / 1000}
            : new long[] {Long.MIN_VALUE, -1_000_001, -1, 0, 1, Long.MAX_VALUE};
    try (BufferAllocator allocator = new RootAllocator();
        TimeStampVector vector = (TimeStampVector) field.createVector(allocator)) {
      vector.allocateNew();
      TimestampAccessor accessor = new TimestampAccessor(vector);
      ArrowTimestampColumnVector rowView = new ArrowTimestampColumnVector(vector);
      for (long value : values) {
        vector.setSafe(0, value);
        vector.setNull(1);
        vector.setValueCount(2);
        TimestampData expected =
            TimestampData.fromInstant(
                Instant.ofEpochSecond(
                    value / perSecond, value % perSecond * (1_000_000_000 / perSecond)));
        assertEquals(expected, accessor.getTimestamp(0));
        assertEquals(expected, rowView.getTimestamp(0, 9));
        assertEquals(expected.getMillisecond(), accessor.maxMillis(2));
      }
      vector.setNull(0);
      assertNull(accessor.maxMillis(2));
      assertNull(accessor.maxMillis(0));
      if (unit == TimeUnit.SECOND) {
        vector.setSafe(0, Long.MAX_VALUE);
        assertThrows(ArithmeticException.class, () -> accessor.getMillis(0));
      }
    }
  }
}
