package tech.streamfusion.paimon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import tech.streamfusion.operator.KeyedUpsertBuffer;
import tech.streamfusion.operator.NativeAllocator;

/**
 * The sink write of a primary-key table fed with routed Arrow batches. Paimon's merge-tree writer
 * takes one row at a time into a sort buffer and creates level-0 files from it, so the batches are
 * kept instead in a native buffer per bucket and written as level-0 files by the native file
 * writer, at a checkpoint or once the task's buffers exceed the table's write buffer size (largest
 * bucket first, as Paimon's memory pool does). Sequence numbers continue from the bucket's committed
 * files exactly as Paimon's writer seeds its own.
 *
 * <p>Compaction stays Paimon's: before Paimon's writer prepares a commit, the new files are handed to
 * it through the entry its dedicated compaction operator uses for files written elsewhere, so it
 * compacts them in this job with its own strategy and reports the results in its commit message.
 * Paimon's writer never sees a row, so its message carries only compaction; the new files are
 * merged into it here. Under {@code write-only} the same hand-off is a no-op and a dedicated
 * compaction job compacts the files through the identical entry.
 */
public final class NativeKeyValueSinkWrite extends StoreSinkWriteImpl {

  private static final long NEW_FILES_SNAPSHOT = Long.MAX_VALUE;

  private static final class BucketBuffer {
    final BinaryRow partition;
    final int bucket;
    final KeyedUpsertBuffer buffer;
    final List<DataFileMeta> pendingFiles = new ArrayList<>();
    long nextSequence;

    BucketBuffer(BinaryRow partition, int bucket, KeyedUpsertBuffer buffer, long nextSequence) {
      this.partition = partition;
      this.bucket = bucket;
      this.buffer = buffer;
      this.nextSequence = nextSequence;
    }
  }

  private FileStoreTable table;
  private final PaimonKeyValueLayout layout;
  private final int kindColumn;
  private final boolean ignoreDelete;
  private final long bufferBudget;
  private final Integer totalBuckets;
  private final Map<BinaryRow, Map<Integer, BucketBuffer>> buffers = new LinkedHashMap<>();
  private NativePaimonKeyValueFileWriter files;

  public NativeKeyValueSinkWrite(
      FileStoreTable table,
      String commitUser,
      StoreSinkWriteState state,
      IOManager ioManager,
      boolean ignorePreviousFiles,
      boolean waitCompaction,
      boolean isStreamingMode,
      MemoryPoolFactory memoryPoolFactory,
      @Nullable MetricGroup metricGroup) {
    super(
        table,
        commitUser,
        state,
        ioManager,
        ignorePreviousFiles,
        waitCompaction,
        isStreamingMode,
        memoryPoolFactory,
        metricGroup);
    CoreOptions options = table.coreOptions();
    this.table = table;
    this.layout = PaimonKeyValueLayout.of(table);
    this.kindColumn = table.rowType().getFieldCount();
    this.ignoreDelete = options.ignoreDelete();
    this.bufferBudget = options.writeBufferSize();
    this.totalBuckets = table.bucketSpec().getNumBuckets();
    this.files = new NativePaimonKeyValueFileWriter(table, layout);
  }

  /** Takes a routed batch (the table's columns plus the hidden row-kind column) for one bucket. */
  public void writeBundle(BinaryRow partition, int bucket, VectorSchemaRoot root) throws IOException {
    if (root.getRowCount() == 0) {
      root.close();
      return;
    }
    BucketBuffer buffer =
        buffers
            .computeIfAbsent(partition, p -> new HashMap<>())
            .computeIfAbsent(bucket, b -> open(partition, bucket));
    buffer.nextSequence += buffer.buffer.push(root, buffer.nextSequence);
    if (bufferedBytes() > bufferBudget) {
      flush(largestBuffer());
    }
  }

  private BucketBuffer open(BinaryRow partition, int bucket) {
    return new BucketBuffer(
        partition,
        bucket,
        new KeyedUpsertBuffer(
            NativeAllocator.SHARED, layout.keyColumns, kindColumn, true, ignoreDelete),
        maxCommittedSequence(partition, bucket) + 1);
  }

  /** The largest sequence number in the bucket's committed files, or -1 for an empty bucket. */
  private long maxCommittedSequence(BinaryRow partition, int bucket) {
    long max = -1;
    for (ManifestEntry entry :
        table.store().newScan().withPartitionBucket(partition, bucket).plan().files()) {
      max = Math.max(max, entry.file().maxSequenceNumber());
    }
    return max;
  }

  private long bufferedBytes() {
    long bytes = 0;
    for (Map<Integer, BucketBuffer> partition : buffers.values()) {
      for (BucketBuffer buffer : partition.values()) {
        bytes += buffer.buffer.bytes();
      }
    }
    return bytes;
  }

  private BucketBuffer largestBuffer() {
    BucketBuffer largest = null;
    for (Map<Integer, BucketBuffer> partition : buffers.values()) {
      for (BucketBuffer buffer : partition.values()) {
        if (largest == null || buffer.buffer.bytes() > largest.buffer.bytes()) {
          largest = buffer;
        }
      }
    }
    return largest;
  }

  private void flush(BucketBuffer buffer) throws IOException {
    KeyedUpsertBuffer.Flushed flushed = buffer.buffer.flush();
    if (flushed != null) {
      buffer.pendingFiles.addAll(files.write(buffer.partition, buffer.bucket, flushed));
    }
  }

  @Override
  public List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
      throws IOException {
    for (Map<Integer, BucketBuffer> partition : buffers.values()) {
      for (BucketBuffer buffer : partition.values()) {
        flush(buffer);
        if (!buffer.pendingFiles.isEmpty()) {
          notifyNewFiles(NEW_FILES_SNAPSHOT, buffer.partition, buffer.bucket, buffer.pendingFiles);
        }
      }
    }
    List<Committable> committables = new ArrayList<>();
    for (Committable committable : super.prepareCommit(waitCompaction, checkpointId)) {
      committables.add(withNewFiles(committable, checkpointId));
    }
    for (Map<Integer, BucketBuffer> partition : buffers.values()) {
      for (BucketBuffer buffer : partition.values()) {
        if (!buffer.pendingFiles.isEmpty()) {
          committables.add(
              new Committable(
                  checkpointId, message(buffer, CompactIncrement.emptyIncrement())));
          buffer.pendingFiles.clear();
        }
      }
    }
    return committables;
  }

  /** Paimon's message for a bucket carries compaction only; the bucket's new files join it. */
  private Committable withNewFiles(Committable committable, long checkpointId) {
    if (!(committable.commitMessage() instanceof CommitMessageImpl)) {
      return committable;
    }
    CommitMessageImpl message = (CommitMessageImpl) committable.commitMessage();
    Map<Integer, BucketBuffer> partition = buffers.get(message.partition());
    BucketBuffer buffer = partition == null ? null : partition.get(message.bucket());
    if (buffer == null || buffer.pendingFiles.isEmpty()) {
      return committable;
    }
    Committable merged =
        new Committable(checkpointId, message(buffer, message.compactIncrement()));
    buffer.pendingFiles.clear();
    return merged;
  }

  private CommitMessageImpl message(BucketBuffer buffer, CompactIncrement compaction) {
    return new CommitMessageImpl(
        buffer.partition,
        buffer.bucket,
        totalBuckets,
        new DataIncrement(
            new ArrayList<>(buffer.pendingFiles), Collections.emptyList(), Collections.emptyList()),
        compaction);
  }

  @Override
  public void replace(FileStoreTable newTable) throws Exception {
    super.replace(newTable);
    table = newTable;
    files = new NativePaimonKeyValueFileWriter(newTable, layout);
  }

  @Override
  public void close() throws Exception {
    for (Map<Integer, BucketBuffer> partition : buffers.values()) {
      for (BucketBuffer buffer : partition.values()) {
        buffer.buffer.close();
      }
    }
    buffers.clear();
    super.close();
  }
}
