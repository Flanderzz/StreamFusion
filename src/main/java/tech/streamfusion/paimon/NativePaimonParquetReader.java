package tech.streamfusion.paimon;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.parquet.NativeParquet;

/** Native Parquet batches over Paimon's authenticated, seekable FileIO. */
public final class NativePaimonParquetReader implements AutoCloseable {
  private final SeekableInputStream input;
  private final byte[] chunk = new byte[64 * 1024];
  private long handle;

  public NativePaimonParquetReader(
      FileIO io, Path path, long length, Schema output, String[] names, int batchRows)
      throws IOException {
    input = io.newInputStream(path);
    try (ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(NativeAllocator.SHARED, output, NativeAllocator.DICTIONARIES, schema);
      handle =
          NativeParquet.createParquetDecoder(
              this, length, schema.memoryAddress(), names, batchRows);
    } catch (Throwable failure) {
      input.close();
      throw failure;
    }
  }

  /** Called synchronously by parquet-rs; the direct buffer is borrowed only for this call. */
  public void readFully(long position, ByteBuffer target) throws IOException {
    input.seek(position);
    while (target.hasRemaining()) {
      int count = input.read(chunk, 0, Math.min(chunk.length, target.remaining()));
      if (count < 0) {
        throw new EOFException("Truncated Paimon Parquet file");
      }
      if (count == 0) {
        throw new IOException("Paimon input made no progress");
      }
      target.put(chunk, 0, count);
    }
  }

  public VectorSchemaRoot next() {
    try (ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try {
        if (!NativeParquet.parquetDecoderNext(
            handle, array.memoryAddress(), schema.memoryAddress())) {
          return null;
        }
        VectorSchemaRoot root =
            VectorSchemaRoot.create(
                Data.importSchema(
                    NativeAllocator.SHARED,
                    ArrowSchema.wrap(schema.memoryAddress()),
                    NativeAllocator.DICTIONARIES),
                NativeAllocator.SHARED);
        try {
          Data.importIntoVectorSchemaRoot(
              NativeAllocator.SHARED,
              ArrowArray.wrap(array.memoryAddress()),
              root,
              NativeAllocator.DICTIONARIES);
          return root;
        } catch (Throwable failure) {
          root.close();
          throw failure;
        }
      } finally {
        if (array.snapshot().release != 0) {
          array.release();
        }
        if (schema.snapshot().release != 0) {
          schema.release();
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (handle != 0) {
        NativeParquet.closeParquetDecoder(handle);
        handle = 0;
      }
    } finally {
      input.close();
    }
  }
}
