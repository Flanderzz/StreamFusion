package tech.streamfusion.arrow;

import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.flink.table.data.TimestampData;

/**
 * A borrowed timestamp view exposing Flink's milliseconds and nanoseconds within the millisecond.
 */
public final class TimestampAccessor {
  private final TimeStampVector vector;
  private final TimeUnit unit;

  public TimestampAccessor(ValueVector vector) {
    if (!(vector instanceof TimeStampVector)) {
      throw new IllegalArgumentException("Expected an Arrow timestamp, got " + vector.getField());
    }
    this.vector = (TimeStampVector) vector;
    this.unit = ((ArrowType.Timestamp) vector.getField().getType()).getUnit();
  }

  public boolean isNull(int row) {
    return vector.isNull(row);
  }

  public long getMillis(int row) {
    return toMillis(vector.get(row));
  }

  public int getNanoOfMillisecond(int row) {
    switch (unit) {
      case MICROSECOND:
        return (int) Math.floorMod(vector.get(row), 1000L) * 1000;
      case NANOSECOND:
        return (int) Math.floorMod(vector.get(row), 1_000_000L);
      default:
        return 0;
    }
  }

  public TimestampData getTimestamp(int row) {
    return TimestampData.fromEpochMillis(getMillis(row), getNanoOfMillisecond(row));
  }

  /** SQL NULL when no row has a timestamp; a raw Long.MIN_VALUE is still a valid timestamp. */
  public Long maxMillis(int rows) {
    long max = Long.MIN_VALUE;
    boolean found = false;
    for (int row = 0; row < rows; row++) {
      if (!isNull(row)) {
        max = Math.max(max, vector.get(row));
        found = true;
      }
    }
    // Unit conversion is monotonic, so only the maximum needs conversion.
    return found ? toMillis(max) : null;
  }

  private long toMillis(long value) {
    switch (unit) {
      case SECOND:
        return Math.multiplyExact(value, 1000L);
      case MILLISECOND:
        return value;
      case MICROSECOND:
        return Math.floorDiv(value, 1000L);
      case NANOSECOND:
        return Math.floorDiv(value, 1_000_000L);
      default:
        throw new IllegalStateException("Unsupported Arrow timestamp unit " + unit);
    }
  }
}
