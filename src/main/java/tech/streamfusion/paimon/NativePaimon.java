package tech.streamfusion.paimon;

import tech.streamfusion.NativeExtensionLoader;

/** JNI entry points owned by the optional Paimon native library. */
public final class NativePaimon {
  static {
    NativeExtensionLoader.load(
        NativePaimon.class,
        "paimon",
        NativePaimon::nativeBuildVersion,
        NativePaimon::liveNativeHandles);
  }

  private NativePaimon() {}

  private static native String nativeBuildVersion();

  public static native String liveNativeHandles();

  public static native long createSnapshotMerger(
      Object provider,
      long inputSchema,
      long outputSchema,
      int keys,
      int runs,
      int rows,
      long budget);

  public static native boolean snapshotMergerNext(long handle, long array, long schema);

  public static native long snapshotMergerPeakBytes(long handle);

  public static native void closeSnapshotMerger(long handle);
}
