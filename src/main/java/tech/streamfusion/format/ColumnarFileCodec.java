package tech.streamfusion.format;

import java.io.IOException;
import java.io.Serializable;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import tech.streamfusion.operator.NativeAllocator;

/**
 * Optional file codecs consume Arrow columns and produce Arrow C Data; the host retains stream
 * ownership.
 */
public interface ColumnarFileCodec extends Serializable {
  Encoder createEncoder(
      org.apache.arrow.vector.types.pojo.Schema schema,
      int[] partitions,
      String[] keys,
      String[] values,
      boolean changelog,
      java.io.OutputStream output)
      throws IOException;

  interface Encoder extends AutoCloseable {
    void write(long array, int[] selected, int offset, int count) throws IOException;

    default void write(VectorSchemaRoot batch, int[] selected, int offset, int count)
        throws IOException {
      var allocator =
          batch.getFieldVectors().isEmpty()
              ? NativeAllocator.SHARED
              : batch.getVector(0).getAllocator();
      try (var array = ArrowArray.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, batch, NativeAllocator.DICTIONARIES, array);
        try {
          write(array.memoryAddress(), selected, offset, count);
        } finally {
          if (array.snapshot().release != 0) array.release();
        }
      }
    }

    long estimatedBytes();

    void finish() throws IOException;

    @Override
    void close();
  }

  long createDecoder(Object input, long length, long schema, String[] names, int batchRows);

  boolean next(long handle, long array, long schema);

  long readerMemory(long handle);

  void closeDecoder(long handle);
}
