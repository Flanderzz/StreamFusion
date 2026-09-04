package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class HostCastFunctionTest {

  private static final List<LogicalType> NOT_NULL_STRINGS =
      List.of(
          new CharType(false, 3),
          new VarCharType(false, 5),
          new VarCharType(false, VarCharType.MAX_LENGTH));

  private static final List<LogicalType> NUMBERS =
      List.of(
          new TinyIntType(),
          new SmallIntType(),
          new IntType(),
          new BigIntType(),
          new FloatType(),
          new DoubleType());

  /**
   * A NOT NULL input type's generated cast has no null guard — it trims the argument directly — so
   * warming the executor with a null failed the operator's open() instead of any row.
   */
  @Test
  void opensForNotNullStringToNumberCasts() {
    for (LogicalType input : NOT_NULL_STRINGS) {
      for (LogicalType target : NUMBERS) {
        assertDoesNotThrow(
            () -> new HostCastFunction(input, target).open(null), input + " -> " + target);
      }
    }
  }

  /** Warming must not consume the executor: the first real row still casts. */
  @Test
  void castsAfterWarmup() {
    HostCastFunction function =
        new HostCastFunction(new VarCharType(false, 5), new IntType());
    function.open(null);
    assertEquals(-7, function.eval("-7"));
  }
}
