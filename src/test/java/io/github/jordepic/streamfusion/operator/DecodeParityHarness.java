package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Shared plumbing for the per-message decode parity tests: runs one message through the native
 * decode operator, renders each row (its kind too, when the format is a changelog) for comparison,
 * and asserts both engines reach the same outcome — identical rows field for field, or both
 * failing. Each test keeps its own Flink referee and option fixtures.
 */
final class DecodeParityHarness {

  interface Decode {
    List<List<Object>> decode() throws Exception;
  }

  private final RowType rowType;
  private final boolean compareRowKinds;

  DecodeParityHarness(RowType rowType, boolean compareRowKinds) {
    this.rowType = rowType;
    this.compareRowKinds = compareRowKinds;
  }

  void assertParity(String message, Decode flinkDecode, Decode nativeDecode) {
    List<List<Object>> expected;
    try {
      expected = flinkDecode.decode();
    } catch (Exception e) {
      expected = null; // Flink failed the message — the native decode must fail it too
    }
    List<List<Object>> actual;
    try {
      actual = nativeDecode.decode();
    } catch (Exception e) {
      actual = null;
    }
    if (expected == null) {
      assertNull(actual, "Flink rejects but native decode accepts: " + message);
      return;
    }
    assertNotNull(actual, "Flink accepts but native decode rejects: " + message);
    assertEquals(expected, actual, "decoded values diverge for: " + message);
  }

  List<List<Object>> nativeDecode(
      NativeFormatProvider provider,
      String message,
      Map<String, String> formatOptions,
      boolean skipErrors)
      throws Exception {
    try (OneInputStreamOperatorTestHarness<byte[], ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(
            new NativeBytesDecodeOperator(
                rowType,
                100,
                provider.createDecoder(
                    new NativeFormatContext(rowType, rowType, formatOptions, skipErrors)),
                0),
            BytePrimitiveArraySerializer.INSTANCE)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(message.getBytes(StandardCharsets.UTF_8)));
      harness.prepareSnapshotPreBarrier(1L);
      List<List<Object>> rows = new ArrayList<>();
      while (!harness.getOutput().isEmpty()) {
        Object event = harness.getOutput().poll();
        if (event instanceof StreamRecord) {
          try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
            for (RowData row : RowDataArrowConverter.read(root, rowType)) {
              rows.add(fields(row));
            }
          }
        }
      }
      return rows;
    }
  }

  /** Each field rendered — plus the row's kind when the format is a changelog. */
  List<Object> fields(RowData row) {
    List<Object> values = new ArrayList<>();
    if (compareRowKinds) {
      values.add(row.getRowKind().shortString());
    }
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      Object value = RowData.createFieldGetter(rowType.getTypeAt(i), i).getFieldOrNull(row);
      values.add(value == null ? null : value.toString());
    }
    return values;
  }
}
