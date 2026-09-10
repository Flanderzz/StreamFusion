package tech.streamfusion.operator;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import tech.streamfusion.Native;

/**
 * Pending changelog rows of one write destination, held natively and merged per key on {@link
 * #flush}. A pushed batch is handed over whole (the buffer owns it afterwards) and its rows take
 * consecutive sequence numbers in arrival order; the flush sorts by key, keeps one row per key (the
 * last or the first to arrive), and returns the rows in a key-value layout: the key columns, the
 * sequence number, the row kind, then every table column. Paimon's merge-on-read requires exactly
 * that shape of a file, so a flush is what a level-0 file is written from.
 */
public final class KeyedUpsertBuffer implements AutoCloseable {

  /** One merged flush: the caller owns {@link #root} and closes it once written. */
  public static final class Flushed {
    public final VectorSchemaRoot root;
    public final long deleteRows;
    public final long minSequence;
    public final long maxSequence;

    Flushed(VectorSchemaRoot root, long deleteRows, long minSequence, long maxSequence) {
      this.root = root;
      this.deleteRows = deleteRows;
      this.minSequence = minSequence;
      this.maxSequence = maxSequence;
    }
  }

  private final BufferAllocator allocator;
  private long handle;

  /**
   * @param keyColumns ordinals of the key columns in the pushed batches
   * @param kindColumn ordinal of the hidden row-kind byte column
   * @param keepLast whether the last row of a key survives (the first otherwise)
   * @param ignoreRetracts whether update-before and delete rows are dropped before merging
   */
  public KeyedUpsertBuffer(
      BufferAllocator allocator,
      int[] keyColumns,
      int kindColumn,
      boolean keepLast,
      boolean ignoreRetracts) {
    this.allocator = allocator;
    this.handle = Native.createKeyedUpsertBuffer(keyColumns, kindColumn, keepLast, ignoreRetracts);
  }

  /** Hands a batch over; its rows take the sequence numbers {@code firstSequence} onwards. */
  public void push(VectorSchemaRoot root, long firstSequence) {
    BufferAllocator rootAllocator =
        root.getFieldVectors().isEmpty() ? allocator : root.getFieldVectors().get(0).getAllocator();
    try (ArrowArray array = ArrowArray.allocateNew(rootAllocator);
        ArrowSchema schema = ArrowSchema.allocateNew(rootAllocator)) {
      Data.exportVectorSchemaRoot(rootAllocator, root, NativeAllocator.DICTIONARIES, array, schema);
      Native.keyedUpsertBufferPush(
          handle, array.memoryAddress(), schema.memoryAddress(), firstSequence);
    } finally {
      root.close();
    }
  }

  public long bytes() {
    return Native.keyedUpsertBufferBytes(handle);
  }

  public long rows() {
    return Native.keyedUpsertBufferRows(handle);
  }

  /** Merges and empties the buffer; {@code null} when no row survives. */
  public Flushed flush() {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      long[] summary =
          Native.keyedUpsertBufferFlush(handle, array.memoryAddress(), schema.memoryAddress());
      if (summary[0] == 0) {
        return null;
      }
      VectorSchemaRoot root =
          Data.importVectorSchemaRoot(allocator, array, schema, NativeAllocator.DICTIONARIES);
      return new Flushed(root, summary[1], summary[2], summary[3]);
    }
  }

  @Override
  public void close() {
    if (handle != 0) {
      Native.closeKeyedUpsertBuffer(handle);
      handle = 0;
    }
  }
}
