package tech.streamfusion.arrow;

import org.apache.flink.table.data.TimestampData;

/** Checked conversion to the engine's signed 64-bit nanosecond timestamp representation. */
public final class TimestampConversion {
  private TimestampConversion() {}

  public static long toNanos(TimestampData timestamp) {
    long millis = timestamp.getMillisecond();
    long fraction = timestamp.getNanoOfMillisecond();
    // The floor millisecond below Long.MIN_VALUE / scale can still have a representable fraction.
    return millis < 0
        ? Math.addExact(Math.multiplyExact(millis + 1, 1_000_000L), fraction - 1_000_000L)
        : Math.addExact(Math.multiplyExact(millis, 1_000_000L), fraction);
  }
}
