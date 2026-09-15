package tech.streamfusion.operator;

import java.io.Serializable;
import org.apache.flink.table.utils.DateTimeUtils;

/** A constant watermark delay whose unit survives planning and source serialization. */
public final class WatermarkDelay implements Serializable {
  private static final long serialVersionUID = 1L;

  private enum Unit {
    MILLISECONDS,
    MONTHS
  }

  private final Unit unit;
  private final long amount;

  private WatermarkDelay(Unit unit, long amount) {
    if (amount < 0) {
      throw new IllegalArgumentException("Watermark delay must be nonnegative");
    }
    this.unit = unit;
    this.amount = amount;
  }

  public static WatermarkDelay millis(long millis) {
    return new WatermarkDelay(Unit.MILLISECONDS, millis);
  }

  public static WatermarkDelay months(int months) {
    return new WatermarkDelay(Unit.MONTHS, months);
  }

  public long subtractFrom(long timestampMillis) {
    return unit == Unit.MONTHS
        ? DateTimeUtils.addMonths(timestampMillis, -(int) amount)
        : timestampMillis - amount;
  }

  @Override
  public String toString() {
    return amount + (unit == Unit.MONTHS ? " MONTHS" : " MILLISECONDS");
  }
}
