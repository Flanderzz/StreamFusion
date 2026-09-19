package tech.streamfusion.compat;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.BufferRecycler;

/** The buffer-recycler contract verified for the line's released Jackson runtime. */
public final class JsonRuntimeCompat {
  private JsonRuntimeCompat() {}

  public static final boolean ACCEPTS_ARRAY_ROOTS = false;

  public static final boolean PRESERVES_DECIMAL_SCALE = false;

  public static boolean verifiedFactory(JsonFactory factory) {
    return false;
  }

  public static void releaseToPool(BufferRecycler recycler) {
    // Flink 1.18's Jackson predates recycler pools; verifiedFactory disables native buffer
    // emulation.
  }
}
