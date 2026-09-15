package tech.streamfusion;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkGuardedDivisionSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT i <> 0 AND 100 / i > 1 FROM n",
      "SELECT i = 0 OR 100 / i > 1 FROM n",
      "SELECT i = 0 OR MOD(100, i) = 0 FROM n",
      "SELECT i FROM n WHERE i <> 0 AND 100 / i > 1",
      "SELECT i FROM n WHERE i = 0 OR 100 / i > 1",
      "SELECT i IS NULL OR i = 0 OR 100 / i > 1 FROM n",
      "SELECT i = 0 OR CAST(100 / i AS BIGINT) > 1 FROM n"
  })
  void fallibleBooleanOperandsUseFlink(String sql) throws Exception {
    NativeParity.assertFallbackReasonContains(FlinkGuardedDivisionSqlHarnessTest::input,
        sql, "row short-circuiting");
  }

  @Test
  void caseStillSelectsRowsNatively() throws Exception {
    NativeParity.assertParity(FlinkGuardedDivisionSqlHarnessTest::input,
        "SELECT CASE WHEN i = 0 THEN 0 ELSE 100 / i END FROM n");
  }

  @Test
  void infallibleBooleanExpressionsStayNative() throws Exception {
    NativeParity.assertParity(FlinkGuardedDivisionSqlHarnessTest::input,
        "SELECT i = 0 OR i > 1, i <> 0 AND i > 1 FROM n");
  }

  private static TableEnvironment input() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView("n", env.fromData(Types.ROW_NAMED(
        new String[] {"i"}, Types.INT), Row.of(0), Row.of(2), Row.of((Object) null)));
    return table;
  }
}
