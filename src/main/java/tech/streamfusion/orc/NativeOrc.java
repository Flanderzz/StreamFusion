package tech.streamfusion.orc;

import tech.streamfusion.NativeExtensionLoader;

/** JNI entry points for optional native Orc encoding and decoding. */
public final class NativeOrc {

  static {
    NativeExtensionLoader.load(
        NativeOrc.class, "orc", NativeOrc::nativeBuildVersion, NativeOrc::liveNativeHandles);
  }

  private NativeOrc() {}

  /** The loaded extension library's StreamFusion build stamp (the loader's version check). */
  private static native String nativeBuildVersion();

  /** Live handles owned by this library, used by the shared leak sentinel. */
  public static native String liveNativeHandles();

  /** Forces initialization of the extension class, including loading its native library. */
  public static boolean isLoaded() {
    return true;
  }

  public static native long createOrcDecoder(
      Object input,
      long length,
      long schemaAddress,
      String[] physicalNames,
      int batchSize,
      String instantTimezone);

  public static native boolean orcDecoderNext(long handle, long arrayAddress, long schemaAddress);

  public static native void closeOrcDecoder(long handle);

  public static native long orcDecoderMaxStripeBytes(long handle);

  public static native long createOrcEncoder(
      long schemaAddress,
      String orcSchema,
      int[] partitionColumns,
      String[] configKeys,
      String[] configValues,
      Object output,
      byte[] chunk);

  public static native void orcEncoderWrite(
      long handle, long inArrayAddress, int[] selectedRows, int rowOffset, int rowCount);

  public static native long orcEncoderEstimatedBytes(long handle);

  public static native void orcEncoderFinish(long handle);

  public static native void closeOrcEncoder(long handle);

  /** Benchmark-only entry points; require the opt-in reader-comparison native feature. */
  public static native long[] compareReaders(
      int backend,
      Object input,
      long length,
      long schemaAddress,
      String[] physicalNames,
      int batchSize,
      boolean verify);

  public static native long[] consumeComparisonBatch(
      long arrayAddress, long schemaAddress, boolean verify);
}
