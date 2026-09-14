package tech.streamfusion.parquet;

import tech.streamfusion.format.ColumnarFileCodec;

/** Adapts the optional Parquet module to the shared columnar file lifecycle. */
public final class ParquetCodec implements ColumnarFileCodec {

  public long createEncoder(
      long schema,
      int[] partitions,
      String[] keys,
      String[] values,
      boolean changelog,
      Object output,
      byte[] chunk) {

    return NativeParquet.createParquetEncoder(
        schema, partitions, keys, values, changelog, output, chunk);
  }

  public void write(long h, long a, int[] rows, int offset, int count) {
    NativeParquet.parquetEncoderWrite(h, a, rows, offset, count);
  }

  public long estimatedBytes(long h) {
    return NativeParquet.parquetEncoderEstimatedBytes(h);
  }

  public void finish(long h) {
    NativeParquet.parquetEncoderFinish(h);
  }

  public void closeEncoder(long h) {
    NativeParquet.closeParquetEncoder(h);
  }

  public long createDecoder(Object in, long length, long schema, String[] names, int rows) {
    return NativeParquet.createParquetDecoder(in, length, schema, names, rows);
  }

  public boolean next(long h, long a, long s) {
    return NativeParquet.parquetDecoderNext(h, a, s);
  }

  public long readerMemory(long h) {
    return NativeParquet.parquetDecoderMaxRowGroupBytes(h);
  }

  public void closeDecoder(long h) {
    NativeParquet.closeParquetDecoder(h);
  }
}
