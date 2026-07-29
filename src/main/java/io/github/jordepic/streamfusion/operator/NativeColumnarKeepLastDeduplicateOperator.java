package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.operator.MiniBatchMetrics.FlushReason;
import io.github.jordepic.streamfusion.planner.NativeConfig;
import io.github.jordepic.streamfusion.state.PaimonNativeStateSupport;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * Columnar eager (push→emit) deduplication: Arrow in, Arrow out. Serves the three non-buffered dedup
 * variants — rowtime keep-last ({@code RowTimeDeduplicateFunction}), proctime keep-last ({@code
 * ProcTimeDeduplicateKeepLastRowFunction}), and proctime keep-first ({@code
 * ProcTimeDeduplicateKeepFirstRowFunction}). Keep-last keeps the winning row per key and emits a
 * retract changelog eagerly on each input batch ({@code +I} for a key's first row, {@code
 * -U}(previous)/{@code +U}(new) on replacement — the kind rides the {@code $row_kind$} column);
 * keep-first emits each key's first row ({@code +I}, insert-only) and drops the rest. A rowtime order
 * keeps the max-rowtime row; proctime uses arrival order. Insert-only input. Keys are co-located by
 * the columnar shuffle; the per-key stored row and the checkpointed handle state live here. (Rowtime
 * keep-first is watermark-buffered — see {@link NativeColumnarDeduplicateOperator}.)
 */
public class NativeColumnarKeepLastDeduplicateOperator
    extends AbstractNativeStatefulOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] partitionColumns;
  private final int rowtimeColumn;
  private final RowType rowType;
  private final boolean generateUpdateBefore;
  private final boolean rowtimeOrdered;
  private final boolean keepFirst;
  private final boolean miniBatch;
  private final long miniBatchSize;
  private final long stateTtlMillis;

  private transient MiniBatchBoundary boundary;
  private transient MiniBatchMetrics miniBatchMetrics;
  private transient BatchCoalescer coalescer;

  public NativeColumnarKeepLastDeduplicateOperator(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int rowtimeColumn,
      RowType rowType,
      boolean generateUpdateBefore,
      boolean rowtimeOrdered,
      boolean keepFirst,
      boolean miniBatch,
      long miniBatchSize,
      long stateTtlMillis,
      int maxParallelism) {
    super("keep-last deduplicate", keyTimestampPrecisions, maxParallelism);
    this.partitionColumns = partitionColumns;
    this.rowtimeColumn = rowtimeColumn;
    this.rowType = rowType;
    this.generateUpdateBefore = generateUpdateBefore;
    this.rowtimeOrdered = rowtimeOrdered;
    this.keepFirst = keepFirst;
    this.miniBatch = miniBatch && !keepFirst;
    this.miniBatchSize = miniBatchSize;
    this.stateTtlMillis = stateTtlMillis;
  }

  @Override
  protected PaimonNativeStateSupport resolvePaimonState(boolean rawStateRestored) {
    return resolvePaimon(
        rawStateRestored,
        () ->
            withRowSchema(rowType, address -> Native.paimonRowStateSupported(address) ? 1L : 0L)
                != 0,
        stateTtlMillis);
  }

  @Override
  protected long createPaimonHandle(PaimonNativeStateSupport paimon) {
    return withRowSchema(
        rowType,
        rowSchemaAddress ->
            Native.createPaimonKeepLastDeduplicator(
                partitionColumns,
                keyTimestampPrecisions(),
                rowtimeColumn,
                rowSchemaAddress,
                generateUpdateBefore,
                rowtimeOrdered,
                keepFirst,
                miniBatch,
                stateTtlMillis,
                getProcessingTimeService().getCurrentProcessingTime(),
                memoryBudgetBytes(),
                paimon.tableDirectory(),
                maxParallelism(),
                NativeConfig.paimonBuckets(),
                NativeConfig.paimonFileFormat(),
                NativeConfig.paimonFileCompression(),
                paimon.sourceDirectories(),
                paimon.sourceSnapshotTokens(),
                paimon.keyGroupStart(),
                paimon.keyGroupEnd(),
                paimon.aligned()));
  }

  @Override
  protected String[] checkpointPaimonHandle() {
    return Native.checkpointPaimonKeepLastDeduplicator(handle);
  }

  @Override
  protected long createHandle() {
    return Native.createKeepLastDeduplicator(
        partitionColumns,
        keyTimestampPrecisions(),
        rowtimeColumn,
        generateUpdateBefore,
        rowtimeOrdered,
        keepFirst,
        miniBatch,
        stateTtlMillis,
        memoryBudgetBytes());
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    return Native.restoreKeepLastDeduplicatorPartitions(
        partitionColumns,
        keyTimestampPrecisions(),
        rowtimeColumn,
        generateUpdateBefore,
        rowtimeOrdered,
        keepFirst,
        miniBatch,
        stateTtlMillis,
        getProcessingTimeService().getCurrentProcessingTime(),
        snapshots,
        memoryBudgetBytes());
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    return Native.snapshotKeepLastDeduplicatorPartitions(
        handle, maxParallelism(), keyTimestampPrecisions());
  }

  @Override
  protected void closeHandle() {
    if (paimonState()) {
      Native.closePaimonKeepLastDeduplicator(handle);
    } else {
      Native.closeKeepLastDeduplicator(handle);
    }
  }

  @Override
  protected long stateBytesHandle() {
    return paimonState()
        ? Native.paimonKeepLastDeduplicatorStateBytes(handle)
        : Native.keepLastDeduplicatorStateBytes(handle);
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (miniBatch) {
      boundary = new MiniBatchBoundary(miniBatchSize);
      miniBatchMetrics = new MiniBatchMetrics(getMetricGroup());
    }
    coalescer = BatchCoalescer.create(getProcessingTimeService(), this::ingest);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    VectorSchemaRoot in = element.getValue().root();
    if (coalescer != null) {
      coalescer.add(in);
    } else {
      ingest(in);
    }
  }

  private void ingest(VectorSchemaRoot in) {
    if (!miniBatch) {
      try {
        push(in);
      } finally {
        in.close();
      }
      publishStateBytes();
      return;
    }
    int rows = in.getRowCount();
    miniBatchMetrics.onPhysicalBatch();
    try {
      int offset = 0;
      while (offset < rows) {
        boolean firstContribution = offset == 0 || boundary.bufferedRows() == 0;
        int length = boundary.nextSliceLength(rows - offset);
        if (length < rows - offset) {
          miniBatchMetrics.onPhysicalBatchSplit();
        }
        if (offset == 0 && length == rows) {
          push(in);
        } else {
          try (VectorSchemaRoot slice = in.slice(offset, length)) {
            push(slice);
          }
        }
        miniBatchMetrics.onSlice(length, firstContribution);
        offset += length;
        if (boundary.onSlice(length)) {
          flushBundle(FlushReason.COUNT);
        }
      }
    } finally {
      in.close();
    }
    publishStateBytes();
  }

  private void push(VectorSchemaRoot in) {
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      // Flink's TtlTimeProvider clock: the processing-time service is System.currentTimeMillis in
      // production and harness-controlled in tests, so expiry is deterministic to test.
      long now = getProcessingTimeService().getCurrentProcessingTime();
      if (paimonState()) {
        Native.pushPaimonKeepLastDeduplicator(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            now,
            outArray.memoryAddress(),
            outSchema.memoryAddress());
      } else {
        Native.pushKeepLastDeduplicator(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            now,
            outArray.memoryAddress(),
            outSchema.memoryAddress());
      }
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      if (out.getRowCount() > 0) {
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
      } else {
        out.close();
      }
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (coalescer != null) {
      coalescer.flush();
    }
    if (miniBatch) {
      flushBundle(FlushReason.WATERMARK);
      publishStateBytes();
    }
    super.processWatermark(mark);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    if (coalescer != null) {
      coalescer.flush();
    }
    if (miniBatch) {
      flushBundle(FlushReason.CHECKPOINT);
    }
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void finish() throws Exception {
    if (coalescer != null) {
      coalescer.flush();
    }
    if (miniBatch) {
      flushBundle(FlushReason.FINISH);
    }
    super.finish();
  }

  private void flushBundle(FlushReason reason) {
    long transientBytes =
        paimonState()
            ? Native.paimonKeepLastDeduplicatorStagingBytes(handle)
            : Native.keepLastDeduplicatorStagingBytes(handle);
    long touchedKeys =
        paimonState()
            ? Native.paimonKeepLastDeduplicatorStagedKeys(handle)
            : Native.keepLastDeduplicatorStagedKeys(handle);
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      if (paimonState()) {
        Native.flushPaimonKeepLastDeduplicator(
            handle, outArray.memoryAddress(), outSchema.memoryAddress());
      } else {
        Native.flushKeepLastDeduplicator(
            handle, outArray.memoryAddress(), outSchema.memoryAddress());
      }
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      int outputRows = out.getRowCount();
      miniBatchMetrics.onFlush(reason, outputRows, touchedKeys, transientBytes);
      if (outputRows > 0) {
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
      } else {
        out.close();
      }
    }
    boundary.reset();
  }

  @Override
  public void close() throws Exception {
    if (coalescer != null) {
      coalescer.close();
      coalescer = null;
    }
    super.close();
  }
}
