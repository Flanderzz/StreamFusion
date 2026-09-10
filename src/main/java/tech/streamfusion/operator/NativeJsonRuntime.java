package tech.streamfusion.operator;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.BufferRecycler;

/**
 * Jackson input-buffer state shared with Flink's SQL/JSON parser on the task thread. Flink's
 * SqlJsonUtils also uses a default JsonFactory, so both factories access the same thread-local
 * pool.
 */
public final class NativeJsonRuntime {
  private static final JsonFactory FACTORY = new JsonFactory();

  private final BufferRecycler recycler = FACTORY._getBufferRecycler();
  private final char[] buffer = recycler.allocCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER);

  public int bufferSize() {
    return buffer.length;
  }

  public void release(int requiredSize) {
    // createParser(String) grows this buffer for inputs of at most 32768 UTF-16 units.
    // Larger inputs use a StringReader with the existing capacity, even across parser calls.
    recycler.releaseCharBuffer(
        BufferRecycler.CHAR_TOKEN_BUFFER,
        requiredSize > buffer.length ? new char[requiredSize] : buffer);
    recycler.releaseToPool();
  }
}
