package tech.streamfusion.format;

import java.io.Serializable;

/** Optional file codecs consume and produce Arrow C Data; the host retains stream ownership. */
public interface ColumnarFileCodec extends Serializable {
  long createEncoder(
      long schema,
      int[] partitions,
      String[] keys,
      String[] values,
      boolean changelog,
      Object output,
      byte[] chunk);

  void write(long handle, long array, int[] selected, int offset, int count);

  long estimatedBytes(long handle);

  void finish(long handle);

  void closeEncoder(long handle);

  long createDecoder(Object input, long length, long schema, String[] names, int batchRows);

  boolean next(long handle, long array, long schema);

  long readerMemory(long handle);

  void closeDecoder(long handle);
}
