package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer;
import org.apache.flink.table.utils.DateTimeUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies Arrow timestamp conversion, candidate reduction and per-split source watermarks. */
class NativeSourceWatermarksTest {

  @Test
  void maxRowtimeSkipsNullsAndFloorsNanosToMillis() {
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = nanoRoot(allocator)) {
      TimeStampNanoVector rowtimes = (TimeStampNanoVector) root.getVector(0);
      rowtimes.setSafe(0, 999_999_999L); // floors to 999ms
      rowtimes.setNull(1);
      rowtimes.setSafe(2, 1_000_000_001L); // floors to 1000ms — the max
      root.setRowCount(3);
      assertEquals(
          1000,
          NativeSourceWatermarks.summarize(root, 0, WatermarkDelay.millis(0)).maxRowtimeMillis);
    }
  }

  @Test
  void maxRowtimeFloorsPreEpochAndSignalsAllNull() {
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = nanoRoot(allocator)) {
      TimeStampNanoVector rowtimes = (TimeStampNanoVector) root.getVector(0);
      rowtimes.setSafe(0, -1L); // pre-epoch: floors to -1ms, not 0
      root.setRowCount(1);
      assertEquals(
          -1, NativeSourceWatermarks.summarize(root, 0, WatermarkDelay.millis(0)).maxRowtimeMillis);

      rowtimes.setSafe(0, Long.MIN_VALUE);
      assertEquals(
          Math.floorDiv(Long.MIN_VALUE, 1_000_000L),
          NativeSourceWatermarks.maxRowtimeMillis(root, 0));

      rowtimes.setNull(0);
      root.setRowCount(1);
      assertEquals(
          Long.MIN_VALUE,
          NativeSourceWatermarks.summarize(root, 0, WatermarkDelay.millis(0)).maxRowtimeMillis);
    }
  }

  @Test
  void maxRowtimeReadsEpochMillisBigintVerbatim() {
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root =
            VectorSchemaRoot.create(
                new Schema(
                    List.of(
                        new Field(
                            "dateTime",
                            FieldType.nullable(new ArrowType.Int(64, true)),
                            List.of()))),
                allocator)) {
      root.allocateNew();
      BigIntVector rowtimes = (BigIntVector) root.getVector(0);
      rowtimes.setSafe(0, 60_000L);
      rowtimes.setNull(1);
      rowtimes.setSafe(2, 90_000L);
      root.setRowCount(3);
      assertEquals(
          90_000,
          NativeSourceWatermarks.summarize(root, 0, WatermarkDelay.millis(0)).maxRowtimeMillis);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 12, 13, Integer.MAX_VALUE})
  void calendarCandidatesMatchFlinkAcrossMonthEndsAndRangeBoundaries(int months) {
    long[] values = {
      Long.MIN_VALUE,
      Long.MAX_VALUE,
      -1,
      0,
      millis("1969-03-31T12:34:56Z"),
      millis("2023-03-31T12:34:56Z"),
      millis("2024-03-30T23:00:00Z"),
      millis("2024-03-31T00:00:00Z"),
      millis("2024-01-31T12:34:56Z")
    };
    try (BufferAllocator allocator = new RootAllocator();
        BigIntVector rowtimes = new BigIntVector("epoch", allocator)) {
      rowtimes.allocateNew(values.length * 2);
      // One non-null row per batch ensures a larger candidate cannot hide a bad conversion.
      try (var root = new VectorSchemaRoot(List.of(rowtimes.getField()), List.of(rowtimes), 2)) {
        for (long value : values) {
          rowtimes.setSafe(0, value);
          rowtimes.setNull(1);
          var result = NativeSourceWatermarks.summarize(root, 0, WatermarkDelay.months(months));
          assertEquals(value, result.maxRowtimeMillis);
          assertEquals(DateTimeUtils.addMonths(value, -months), result.maxWatermarkMillis);
        }
      }
    }
  }

  @Test
  void sourceCallbackUsesCalendarCandidateAfterDownstreamClosesTheArrowRoot() {
    var generator = NativeSourceWatermarks.strategy(0).createWatermarkGenerator(null);
    CapturingOutput output = new CapturingOutput();
    try (BufferAllocator allocator = new RootAllocator()) {
      NativeSourceRecord record = calendarRecord(allocator);
      assertEquals(millis("2024-03-31T00:00:00Z"), record.maxRowtimeMillis());
      record.batch().root().close();
      assertEquals(0, allocator.getAllocatedMemory());
      generator.onEvent(record.batch(), record.maxRowtimeMillis(), output);
      generator.onPeriodicEmit(output);
      assertEquals(List.of(millis("2024-02-29T23:00:00Z")), output.watermarks);

      var nullRoot = nanoRoot(allocator);
      nullRoot.getVector(0).setNull(0);
      nullRoot.setRowCount(1);
      var allNull = NativeSourceRecord.fromRoot(nullRoot, 4, 0, WatermarkDelay.months(1));
      assertEquals(Long.MIN_VALUE, allNull.batch().sourceWatermarkMillis());
      allNull.batch().root().close();
      generator.onEvent(allNull.batch(), Long.MIN_VALUE, output);
      generator.onPeriodicEmit(output);
      assertEquals(
          List.of(millis("2024-02-29T23:00:00Z"), millis("2024-02-29T23:00:00Z")),
          output.watermarks);
    }
  }

  @Test
  void splitMinimumAndIdlenessStillUseFlinksCoordination() {
    CapturingOutput output = new CapturingOutput();
    var multiplexer = new WatermarkOutputMultiplexer(output);
    multiplexer.registerNewOutput("a");
    multiplexer.registerNewOutput("b");
    var first = NativeSourceWatermarks.strategy(0).createWatermarkGenerator(null);
    var second = NativeSourceWatermarks.strategy(0).createWatermarkGenerator(null);
    try (BufferAllocator allocator = new RootAllocator()) {
      var record = calendarRecord(allocator);
      record.batch().root().close();
      first.onEvent(record.batch(), record.maxRowtimeMillis(), multiplexer.getImmediateOutput("a"));
      first.onPeriodicEmit(multiplexer.getDeferredOutput("a"));
      second.onPeriodicEmit(multiplexer.getDeferredOutput("b"));
      multiplexer.onPeriodicEmit();
      assertEquals(List.of(), output.watermarks);
      multiplexer.getImmediateOutput("b").markIdle();
      assertEquals(List.of(millis("2024-02-29T23:00:00Z")), output.watermarks);
    }
  }

  @Test
  void fixedDelayIsAppliedBeforeMaximumWhenLongArithmeticWraps() {
    try (BufferAllocator allocator = new RootAllocator();
        BigIntVector vector = new BigIntVector("epoch", allocator)) {
      vector.allocateNew(2);
      vector.set(0, Long.MIN_VALUE);
      vector.set(1, 0);
      try (var root = new VectorSchemaRoot(List.of(vector.getField()), List.of(vector), 2)) {
        var result = NativeSourceWatermarks.summarize(root, 0, WatermarkDelay.millis(1));
        assertEquals(0, result.maxRowtimeMillis);
        assertEquals(Long.MAX_VALUE, result.maxWatermarkMillis);
      }
    }
  }

  private static NativeSourceRecord calendarRecord(BufferAllocator allocator) {
    var root = nanoRoot(allocator);
    var timestamps = (TimeStampNanoVector) root.getVector(0);
    timestamps.setSafe(0, millis("2024-03-30T23:00:00Z") * 1_000_000);
    timestamps.setSafe(1, millis("2024-03-31T00:00:00Z") * 1_000_000);
    timestamps.setNull(2);
    root.setRowCount(3);
    return NativeSourceRecord.fromRoot(root, 3, 0, WatermarkDelay.months(1));
  }

  private static long millis(String timestamp) {
    return Instant.parse(timestamp).toEpochMilli();
  }

  private static final class CapturingOutput implements WatermarkOutput {
    final List<Long> watermarks = new ArrayList<>();

    @Override
    public void emitWatermark(Watermark watermark) {
      watermarks.add(watermark.getTimestamp());
    }

    @Override
    public void markIdle() {}

    @Override
    public void markActive() {}
  }

  private static VectorSchemaRoot nanoRoot(BufferAllocator allocator) {
    VectorSchemaRoot root =
        VectorSchemaRoot.create(
            new Schema(
                List.of(
                    new Field(
                        "ts",
                        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)),
                        List.of()))),
            allocator);
    root.allocateNew();
    return root;
  }
}
