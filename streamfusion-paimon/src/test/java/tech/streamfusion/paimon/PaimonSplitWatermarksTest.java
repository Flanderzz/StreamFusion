package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeSourceRecord;
import tech.streamfusion.operator.WatermarkDelay;

class PaimonSplitWatermarksTest {
  @Test
  void finalSplitFlushUsesCandidatesAfterCollectAndBeforeRelease() {
    CapturingOutput output = new CapturingOutput();
    var watermarks = new PaimonSplitWatermarks(output);
    try (var allocator = new RootAllocator()) {
      var first = watermarks.createOutputForSplit("first");
      var second = watermarks.createOutputForSplit("second");
      var calendar =
          record(
              allocator,
              WatermarkDelay.months(1),
              millis("2024-03-30T23:00:00Z"),
              millis("2024-03-31T00:00:00Z"));
      calendar.emit(first, offset -> assertEquals(2, offset));
      record(allocator, WatermarkDelay.millis(100), 900L).emit(second, offset -> {});
      record(allocator, WatermarkDelay.months(1), (Long) null).emit(first, offset -> {});
      assertEquals(0, allocator.getAllocatedMemory());
      assertEquals(3, output.events.size());
      watermarks.releaseOutputForSplit("second");
      watermarks.releaseOutputForSplit("first");
      assertEquals(
          List.of(
              "row:" + millis("2024-03-31T00:00:00Z"),
              "row:900",
              "row:null",
              "wm:800",
              "release:second",
              "wm:" + millis("2024-02-29T23:00:00Z"),
              "release:first"),
          output.events);
    }
  }

  @Test
  void nullOnlyAndEmptySplitsDoNotInventWatermarks() {
    CapturingOutput output = new CapturingOutput();
    var watermarks = new PaimonSplitWatermarks(output);
    try (var allocator = new RootAllocator()) {
      record(allocator, WatermarkDelay.months(1), (Long) null)
          .emit(watermarks.createOutputForSplit("nulls"), offset -> {});
      watermarks.releaseOutputForSplit("nulls");
      watermarks.createOutputForSplit("empty");
      watermarks.releaseOutputForSplit("empty");
      assertEquals(List.of("row:null", "release:nulls", "release:empty"), output.events);
    }
  }

  private static NativeSourceRecord record(
      RootAllocator allocator, WatermarkDelay delay, Long... values) {
    var vector = new BigIntVector("epoch", allocator);
    vector.allocateNew(values.length);
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) vector.setNull(i);
      else vector.set(i, values[i]);
    }
    var root = new VectorSchemaRoot(List.of(vector.getField()), List.of(vector), values.length);
    return NativeSourceRecord.fromRoot(root, values.length, 0, delay);
  }

  private static long millis(String timestamp) {
    return Instant.parse(timestamp).toEpochMilli();
  }

  private static final class CapturingOutput implements ReaderOutput<ArrowBatch> {
    final List<String> events = new ArrayList<>();

    @Override
    public SourceOutput<ArrowBatch> createOutputForSplit(String id) {
      return this;
    }

    @Override
    public void releaseOutputForSplit(String id) {
      events.add("release:" + id);
    }

    @Override
    public void collect(ArrowBatch record) {
      events.add("row:null");
      record.root().close();
    }

    @Override
    public void collect(ArrowBatch record, long timestamp) {
      events.add("row:" + timestamp);
      record.root().close();
    }

    @Override
    public void emitWatermark(Watermark watermark) {
      events.add("wm:" + watermark.getTimestamp());
    }

    @Override
    public void markIdle() {}

    @Override
    public void markActive() {}
  }
}
