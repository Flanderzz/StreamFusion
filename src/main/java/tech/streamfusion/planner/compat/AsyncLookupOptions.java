package tech.streamfusion.planner.compat;

/**
 * The async-lookup settings a lookup join was planned with, carried opaquely.
 *
 * <p>Flink renamed the enclosing utility between 2.1 and 2.2 but kept the fields, so the two values
 * the native operator needs are copied out here and the original is retained only for the codegen
 * call that still requires it.
 */
public final class AsyncLookupOptions {

  private final int bufferCapacity;
  private final boolean keyOrdered;

  public AsyncLookupOptions(int bufferCapacity, boolean keyOrdered) {
    this.bufferCapacity = bufferCapacity;
    this.keyOrdered = keyOrdered;
  }

  public int bufferCapacity() {
    return bufferCapacity;
  }

  public boolean keyOrdered() {
    return keyOrdered;
  }
}
