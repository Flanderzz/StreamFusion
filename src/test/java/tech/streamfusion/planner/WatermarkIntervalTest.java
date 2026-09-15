package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;

class WatermarkIntervalTest {
  private final RexBuilder rex = new RexBuilder(new JavaTypeFactoryImpl());

  @Test
  void intervalFamilyDeterminesTheUnit() {
    assertEquals("12 MONTHS", parse("12", TimeUnit.YEAR).toString());
    assertEquals("1 MONTHS", parse("1", TimeUnit.MONTH).toString());
    assertEquals("86400000 MILLISECONDS", parse("86400000", TimeUnit.DAY).toString());
    assertEquals("0 MILLISECONDS", parse("0", TimeUnit.SECOND).toString());
    assertEquals("2147483647 MONTHS", parse("2147483647", TimeUnit.MONTH).toString());
  }

  @Test
  void nonIntervalsNegativeFractionalAndTruncatedValuesAreNotAdmitted() {
    assertNull(WatermarkInterval.parse(rex.makeLiteral("one month")));
    assertNull(WatermarkInterval.parse(rex.makeExactLiteral(BigDecimal.ONE)));
    assertNull(parse("-1", TimeUnit.MONTH));
    assertNull(parse("-1", TimeUnit.SECOND));
    assertNull(parse("1.5", TimeUnit.MONTH));
    assertNull(parse("2147483648", TimeUnit.MONTH));
    assertNull(parse("4294967297", TimeUnit.MONTH));
    assertNull(parse("9223372036854775808", TimeUnit.SECOND));
  }

  private tech.streamfusion.operator.WatermarkDelay parse(String value, TimeUnit unit) {
    return WatermarkInterval.parse(
        rex.makeIntervalLiteral(
            new BigDecimal(value), new SqlIntervalQualifier(unit, null, SqlParserPos.ZERO)));
  }
}
