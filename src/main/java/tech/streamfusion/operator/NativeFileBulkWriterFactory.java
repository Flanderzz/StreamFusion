package tech.streamfusion.operator;

import java.io.IOException;
import java.lang.ref.Cleaner;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.format.ColumnarFileCodec;

/**
 * Creates the native columnar writers behind the sink's part files. Each part file pairs a native
 * Arrow file writer with the Flink {@link FSDataOutputStream} the bucket opened, so the bytes
 * travel Flink's own recoverable-stream path — any Flink filesystem, the host's exactly-once commit
 * — while the encoding never touches Java rows. Encoded bytes cross JNI through a reusable one-MiB
 * array.
 */
public class NativeFileBulkWriterFactory implements BulkWriter.Factory<PartitionedArrowBatch> {

  private static final int DRAIN_CHUNK_BYTES = 1 << 20;

  private final ColumnarFileCodec codec;
  private final RowType rowType;
  private final int[] partitionColumns;
  private final String[] configKeys;
  private final String[] configValues;
  private final boolean changelog;

  public NativeFileBulkWriterFactory(
      ColumnarFileCodec codec,
      RowType rowType,
      int[] partitionColumns,
      String[] configKeys,
      String[] configValues) {
    this(codec, rowType, partitionColumns, configKeys, configValues, false);
  }

  public NativeFileBulkWriterFactory(
      ColumnarFileCodec codec,
      RowType rowType,
      int[] partitionColumns,
      String[] configKeys,
      String[] configValues,
      boolean changelog) {
    this.codec = codec;
    this.rowType = rowType;
    this.partitionColumns = partitionColumns;
    this.configKeys = configKeys;
    this.configValues = configValues;
    this.changelog = changelog;
  }

  @Override
  public BulkWriter<PartitionedArrowBatch> create(FSDataOutputStream out) {
    BufferAllocator allocator = NativeAllocator.SHARED;
    byte[] chunk = new byte[DRAIN_CHUNK_BYTES];
    long encoder;
    try (ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(
          allocator, ArrowConversion.toArrowSchema(rowType), NativeAllocator.DICTIONARIES, schema);
      encoder =
          codec.createEncoder(
              schema.memoryAddress(),
              partitionColumns,
              configKeys,
              configValues,
              changelog,
              out,
              chunk);
    }
    return new NativeFileBulkWriter(codec, encoder);
  }

  private static final class NativeFileBulkWriter implements BulkWriter<PartitionedArrowBatch> {

    private static final Cleaner ABANDONED = Cleaner.create();

    private final ColumnarFileCodec codec;
    private final long encoder;
    private final Backstop backstop;
    private final Cleaner.Cleanable cleanable;

    private NativeFileBulkWriter(ColumnarFileCodec codec, long encoder) {
      this.codec = codec;
      this.encoder = encoder;
      // Flink disposes an in-progress part file by closing only its stream — the bulk writer is
      // dropped without finish() — so a backstop frees the native encoder when that happens.
      this.backstop = new Backstop(codec, encoder);
      cleanable = ABANDONED.register(this, backstop);
    }

    @Override
    public void addElement(PartitionedArrowBatch element) throws IOException {
      VectorSchemaRoot batch = element.root();
      BufferAllocator batchAllocator =
          batch.getFieldVectors().isEmpty()
              ? NativeAllocator.SHARED
              : batch.getFieldVectors().get(0).getAllocator();
      try (ArrowArray array = ArrowArray.allocateNew(batchAllocator)) {
        Data.exportVectorSchemaRoot(batchAllocator, batch, NativeAllocator.DICTIONARIES, array);
        codec.write(encoder, array.memoryAddress(), new int[0], 0, -1);
      } finally {
        batch.close();
      }
    }

    @Override
    public void flush() throws IOException {
      // The codec owns stripe or row-group finalization; Flink calls finish before publishing a
      // part file.
    }

    @Override
    public void finish() throws IOException {
      if (backstop.released) return;
      try {
        codec.finish(encoder);
      } finally {
        cleanable.clean();
      }
    }

    /** Frees the encoder of a part file disposed without finish; must not reference its writer. */
    private static final class Backstop implements Runnable {

      private final ColumnarFileCodec codec;
      private final long encoder;
      private volatile boolean released;

      private Backstop(ColumnarFileCodec codec, long encoder) {
        this.codec = codec;
        this.encoder = encoder;
      }

      @Override
      public void run() {
        if (!released) {
          released = true;
          codec.closeEncoder(encoder);
        }
      }
    }
  }
}
