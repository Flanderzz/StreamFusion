package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.arrow.ArrowConversion;
import io.github.jordepic.streamfusion.planner.NativeConfig;
import io.github.jordepic.streamfusion.state.PaimonNativeStateSupport;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * Columnar event-time {@code OVER (… ORDER BY rt RANGE UNBOUNDED PRECEDING)} aggregation: Arrow in,
 * Arrow out. Each input batch is buffered natively; on a watermark the native aggregator emits the
 * rows it has completed (rowtime past the watermark) with the running aggregate column(s) appended,
 * the input columns passing through — so the data stays columnar end to end. The buffering, the
 * per-key running fold, and the late-data drop all live in the native operator; this layer only
 * moves batches across the bridge and owns the handle's checkpointed state. On the Paimon backend
 * the pending rows and the per-key fold state live in the persistent store (write buffers + disk
 * tables) and the watermark firing is a range read over both; memory state travels as raw
 * keyed-state blobs.
 */
public class NativeOverAggregateOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;
  private final int frameKind;
  private final long frameOffset;
  private final boolean proctime;
  private final int[] keyTimestampPrecisions;
  private final RowType rowType;
  private final int maxParallelism;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient boolean paimonState;
  private transient ManagedMemoryBudget memoryBudget;

  public NativeOverAggregateOperator(
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] valueTypes,
      int[] aggregateKinds,
      int frameKind,
      long frameOffset,
      boolean proctime,
      int[] keyTimestampPrecisions,
      RowType rowType,
      int maxParallelism) {
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.valueTypes = valueTypes;
    this.aggregateKinds = aggregateKinds;
    this.frameKind = frameKind;
    this.frameOffset = frameOffset;
    this.proctime = proctime;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
    this.rowType = rowType;
    if (maxParallelism <= 0) {
      throw new IllegalArgumentException("native OVER state requires a positive max parallelism");
    }
    this.maxParallelism = maxParallelism;
  }

  @Override
  protected boolean isUsingCustomRawKeyedState() {
    return true;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    java.util.List<byte[]> rawSnapshots = RawKeyedState.restore(context);
    memoryBudget = ManagedMemoryBudget.reserveFor(this);
    PaimonNativeStateSupport paimon =
        PaimonNativeStateSupport.resolve(
            getKeyedStateBackend(),
            "over aggregate",
            !rawSnapshots.isEmpty(),
            () ->
                withRowSchema(
                        address ->
                            Native.paimonOverStateSupported(
                                    address, valueTypes, aggregateKinds, frameKind, proctime)
                                ? 1L
                                : 0L)
                    != 0);
    paimonState = paimon != null;
    if (paimonState) {
      handle =
          withRowSchema(
              rowSchemaAddress ->
                  Native.createPaimonOverAggregator(
                      valueTypes,
                      aggregateKinds,
                      timeColumn,
                      valueColumns,
                      keyColumns,
                      frameKind,
                      frameOffset,
                      keyTimestampPrecisions,
                      rowSchemaAddress,
                      memoryBudget.bytes(),
                      paimon.tableDirectory(),
                      maxParallelism,
                      NativeConfig.paimonBuckets(),
                      NativeConfig.paimonFileFormat(),
                      NativeConfig.paimonFileCompression(),
                      paimon.sourceDirectories(),
                      paimon.sourceSnapshotTokens(),
                      paimon.keyGroupStart(),
                      paimon.keyGroupEnd(),
                      paimon.aligned()));
      long nativeHandle = handle;
      paimon.register(() -> Native.checkpointPaimonOverAggregator(nativeHandle));
      return;
    }
    if (!rawSnapshots.isEmpty()) {
      handle =
          Native.restoreOverAggregatorPartitions(
              valueTypes,
              aggregateKinds,
              timeColumn,
              valueColumns,
              keyColumns,
              frameKind,
              frameOffset,
              proctime,
              rawSnapshots.toArray(new byte[0][]),
              memoryBudget.bytes());
    } else {
      handle =
          Native.createOverAggregator(
                valueTypes,
                aggregateKinds,
                timeColumn,
                valueColumns,
                keyColumns,
                frameKind,
                frameOffset,
                proctime,
              memoryBudget.bytes());
    }
  }

  /**
   * Exports the input row type as an FFI Arrow schema for the duration of one native call; the
   * native side consumes the schema contents, the wrapper struct is released here.
   */
  private long withRowSchema(java.util.function.LongUnaryOperator call) {
    try (ArrowSchema rowSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED,
          ArrowConversion.toArrowSchema(rowType),
          NativeAllocator.DICTIONARIES,
          rowSchema);
      return call.applyAsLong(rowSchema.memoryAddress());
    }
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot in = element.getValue().root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      if (proctime) {
        // Proctime: fold in arrival order and emit this batch's rows immediately (no watermark).
        try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
            ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
          Native.pushProctimeOverAggregator(
              handle,
              inArray.memoryAddress(),
              inSchema.memoryAddress(),
              outArray.memoryAddress(),
              outSchema.memoryAddress());
          VectorSchemaRoot out =
              Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
          if (out.getRowCount() > 0) {
            output.collect(new StreamRecord<>(new ArrowBatch(out)));
          } else {
            out.close();
          }
        }
      } else if (paimonState) {
        // Rowtime on the Paimon backend: rows stage into the pending write buffer.
        Native.pushPaimonOverAggregator(handle, inArray.memoryAddress(), inSchema.memoryAddress());
      } else {
        // Rowtime: the native aggregator imports and keeps the batch (buffered until a watermark
        // completes these rows), so this side hands it off and closes its own view.
        Native.pushOverAggregator(handle, inArray.memoryAddress(), inSchema.memoryAddress());
      }
    } finally {
      in.close();
    }
    publishStateBytes();
  }

  /** Samples the native state size for the operator's gauges; task-thread only. */
  private void publishStateBytes() {
    if (memoryBudget.bounded()) {
      memoryBudget.publishStateBytes(
          paimonState
              ? Native.paimonOverAggregatorStateBytes(handle)
              : Native.overAggregatorStateBytes(handle));
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (proctime) {
      super.processWatermark(mark); // proctime emits eagerly in processElement; nothing to flush
      return;
    }
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      if (paimonState) {
        Native.flushPaimonOverAggregator(
            handle, mark.getTimestamp(), array.memoryAddress(), schema.memoryAddress());
      } else {
        Native.flushOverAggregator(
            handle, mark.getTimestamp(), array.memoryAddress(), schema.memoryAddress());
      }
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close(); // nothing completed this watermark
      }
    }
    publishStateBytes();
    super.processWatermark(mark);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    // Paimon state checkpoints through the keyed state backend's snapshot (an incremental Paimon
    // commit); only memory state travels as raw keyed-state blobs.
    if (!paimonState) {
      RawKeyedState.snapshotPartitions(
          context,
          Native.snapshotOverAggregatorPartitions(
              handle, maxParallelism, keyTimestampPrecisions));
    }
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      if (paimonState) {
        Native.closePaimonOverAggregator(handle);
      } else {
        Native.closeOverAggregator(handle);
      }
      handle = 0;
    }
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
    super.close();
  }
}
