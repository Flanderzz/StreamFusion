package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkDecimalRemainderSqlHarnessTest {
  private static final String SQL = "SELECT MOD(a, b) FROM t";

  @ParameterizedTest
  @ValueSource(
      strings = {
        "10000000000000000000000000000000000000",
        "-10000000000000000000000000000000000000"
      })
  void integralQuotientMustFitTheMathContext(String value) {
    for (boolean nativeRun : new boolean[] {false, true}) {
      TableEnvironment table = environment(value, "0.00000000000000000000000000000000000003");
      if (nativeRun) {
        assertTrue(NativePlanner.explain(table, SQL).contains("NativeCalc"));
        NativePlanner.install(table);
      }
      Exception failure =
          assertThrows(
              Exception.class,
              () -> {
                try (var rows = table.executeSql(SQL).collect()) {
                  while (rows.hasNext()) {
                    rows.next();
                  }
                }
              });
      StringBuilder causes = new StringBuilder();
      for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
        causes.append(cause.getMessage()).append('\n');
      }
      assertTrue(causes.toString().contains("Division impossible"), causes.toString());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"1", "2", "4", "5"})
  void largeIntegralQuotientMayHaveRemovableTrailingZeros(String divisor) throws Exception {
    NativeParity.assertParity(
        () ->
            environment(
                "10000000000000000000000000000000000000",
                "0.0000000000000000000000000000000000000" + divisor),
        SQL);
  }

  private static TableEnvironment environment(String left, String right) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "t",
        env.fromData(
            Types.ROW_NAMED(new String[] {"a", "b"}, Types.BIG_DEC, Types.BIG_DEC),
            Row.of(new BigDecimal(left), new BigDecimal(right))),
        Schema.newBuilder()
            .column("a", DataTypes.DECIMAL(38, 0))
            .column("b", DataTypes.DECIMAL(38, 38))
            .build());
    return table;
  }
}
