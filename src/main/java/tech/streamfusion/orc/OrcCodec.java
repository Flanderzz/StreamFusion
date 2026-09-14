package tech.streamfusion.orc;

import tech.streamfusion.format.ColumnarFileCodec;

/** Adapts the optional Orc module to the shared columnar file lifecycle. */
public final class OrcCodec implements ColumnarFileCodec {
  private final String schema;
  private final boolean legacyTimestamp;

  public OrcCodec(String schema) {
    this(schema, false);
  }

  public OrcCodec(String schema, boolean legacyTimestamp) {
    this.schema = schema;
    this.legacyTimestamp = legacyTimestamp;
  }

  public long createEncoder(
      long schema,
      int[] partitions,
      String[] keys,
      String[] values,
      boolean changelog,
      Object output,
      byte[] chunk) {
    if (changelog) throw new IllegalArgumentException("ORC filesystem sinks require inserts");
    return NativeOrc.createOrcEncoder(schema, this.schema, partitions, keys, values, output, chunk);
  }

  public void write(long h, long a, int[] rows, int offset, int count) {
    NativeOrc.orcEncoderWrite(h, a, rows, offset, count);
  }

  public long estimatedBytes(long h) {
    return NativeOrc.orcEncoderEstimatedBytes(h);
  }

  public void finish(long h) {
    NativeOrc.orcEncoderFinish(h);
  }

  public void closeEncoder(long h) {
    NativeOrc.closeOrcEncoder(h);
  }

  public long createDecoder(Object in, long length, long schema, String[] names, int rows) {
    return NativeOrc.createOrcDecoder(
        in,
        length,
        schema,
        names,
        rows,
        legacyTimestamp ? java.util.TimeZone.getDefault().getID() : "");
  }

  public boolean next(long h, long a, long s) {
    return NativeOrc.orcDecoderNext(h, a, s);
  }

  public long readerMemory(long h) {
    return NativeOrc.orcDecoderMaxStripeBytes(h);
  }

  public void closeDecoder(long h) {
    NativeOrc.closeOrcDecoder(h);
  }
}
