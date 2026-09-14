package tech.streamfusion.paimon;

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.types.RowType;
import tech.streamfusion.format.ColumnarFileCodec;
import tech.streamfusion.operator.NativeAllocator;

/**
 * A Paimon data-file writer that encodes Arrow bundles natively. The first record decides the file:
 * an {@link ArrowBatchBundle} opens the native encoder over Paimon's output stream and every later
 * bundle is appended to it column-wise, while a plain row hands the whole file to Paimon's stock
 * file writer. Paimon's released append writer feeds a bundle to its format writer one cursor row
 * at a time, so the cursor path recognises each bundle once and encodes it whole; a Paimon that
 * passes the bundle through takes the direct path. Compaction rewrites arrive as plain rows and
 * take the stock path; the native append spill buffer drains Arrow bundles. The footer stays a
 * standard file footer, so Paimon extracts statistics from it exactly as from its own files.
 */
public final class NativePaimonFileWriter implements BundleFormatWriter {

  private static final int DRAIN_CHUNK_BYTES = 1 << 20;

  private enum Mode {
    UNDECIDED,
    NATIVE,
    STOCK
  }

  private final RowType writeType;
  private final ColumnarFileCodec codec;
  private final String[] keys, values;
  private final PositionOutputStream out;
  private final String compression;
  private final FormatWriterFactory stockFactory;
  @Nullable private PaimonParquetFloatingStats floatingStats;

  private Mode mode = Mode.UNDECIDED;
  private long encoder;
  private boolean closed;
  @Nullable private FormatWriter stock;
  @Nullable private ArrowBatchBundle lastBundle;

  NativePaimonFileWriter(
      RowType writeType,
      ColumnarFileCodec codec,
      String[] keys,
      String[] values,
      PositionOutputStream out,
      String compression,
      FormatWriterFactory stockFactory) {
    this.writeType = writeType;
    this.codec = codec;
    this.keys = keys;
    this.values = values;
    this.out = out;
    this.compression = compression;
    this.stockFactory = stockFactory;
  }

  NativePaimonFileWriter withParquetFloatingStats() {
    floatingStats = new PaimonParquetFloatingStats(writeType);
    return this;
  }

  @Override
  public void writeBundle(BundleRecords bundle) throws IOException {
    if (closed) throw new IllegalStateException("Paimon file writer is closed");
    if (bundle instanceof ArrowBatchBundle) {
      writeNative((ArrowBatchBundle) bundle);
      return;
    }
    for (InternalRow row : bundle) {
      addElement(row);
    }
  }

  @Override
  public void addElement(InternalRow row) throws IOException {
    if (closed) throw new IllegalStateException("Paimon file writer is closed");
    if (row instanceof ArrowBatchBundle.Cursor) {
      writeNative(((ArrowBatchBundle.Cursor) row).bundle());
    } else {
      stock().addElement(row);
    }
  }

  private void writeNative(ArrowBatchBundle bundle) throws IOException {
    if (mode == Mode.STOCK) {
      throw new IllegalStateException(
          "an Arrow bundle reached a Paimon data file already written row by row");
    }
    if (bundle == lastBundle) {
      return;
    }
    VectorSchemaRoot root = bundle.root();
    if (mode == Mode.UNDECIDED) {
      openEncoder(root);
    }
    BufferAllocator allocator = allocatorOf(root);
    try (ArrowArray array = ArrowArray.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array);
      codec.write(
          encoder, array.memoryAddress(), new int[0], bundle.rowOffset(), (int) bundle.rowCount());
    }
    if (floatingStats != null)
      floatingStats.collect(root, bundle.rowOffset(), (int) bundle.rowCount());
    lastBundle = bundle;
  }

  private void openEncoder(VectorSchemaRoot root) {
    BufferAllocator allocator = allocatorOf(root);
    try (ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(
          allocator,
          PaimonArrowFields.annotate(root.getSchema(), writeType),
          NativeAllocator.DICTIONARIES,
          schema);
      encoder =
          codec.createEncoder(
              schema.memoryAddress(),
              new int[0],
              keys,
              values,
              false,
              out,
              new byte[DRAIN_CHUNK_BYTES]);
    }
    mode = Mode.NATIVE;
  }

  private static BufferAllocator allocatorOf(VectorSchemaRoot root) {
    return root.getFieldVectors().isEmpty()
        ? NativeAllocator.SHARED
        : root.getFieldVectors().get(0).getAllocator();
  }

  private FormatWriter stock() throws IOException {
    if (mode == Mode.NATIVE) {
      throw new IllegalStateException(
          "a plain row reached a Paimon data file already written from Arrow bundles");
    }
    if (stock == null) {
      stock = stockFactory.create(out, compression);
      mode = Mode.STOCK;
    }
    return stock;
  }

  @Override
  public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
    return switch (mode) {
      case NATIVE -> suggestedCheck && codec.estimatedBytes(encoder) >= targetSize;
      case STOCK -> stock.reachTargetSize(suggestedCheck, targetSize);
      case UNDECIDED -> false;
    };
  }

  @Nullable
  @Override
  public Object writerMetadata() {
    return mode == Mode.STOCK ? stock.writerMetadata() : floatingStats;
  }

  /** Whether this file was written from Arrow bundles rather than by Paimon's stock writer. */
  public boolean wroteNatively() {
    return mode == Mode.NATIVE;
  }

  @Override
  public void close() throws IOException {
    if (closed) return;
    closed = true;
    switch (mode) {
      case NATIVE -> {
        try {
          codec.finish(encoder);
        } finally {
          codec.closeEncoder(encoder);
          encoder = 0;
        }
      }
      case STOCK -> stock.close();
      case UNDECIDED -> stock().close();
    }
  }
}
