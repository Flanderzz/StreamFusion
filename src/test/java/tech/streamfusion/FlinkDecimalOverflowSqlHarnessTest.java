package tech.streamfusion;

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

/** Decimal overflow must be SQL NULL, including when observed by a downstream expression. */
class FlinkDecimalOverflowSqlHarnessTest {

  @ParameterizedTest
  @ValueSource(strings = {
    "SELECT id, CAST(a AS DECIMAL(5, 2)) FROM t",
    "SELECT id, CAST(a AS DECIMAL(5, 2)) IS NULL FROM t",
    "SELECT id FROM t WHERE CAST(a AS DECIMAL(5, 2)) IS NULL",
    "SELECT id, CAST(a AS DECIMAL(38, 38)) FROM t"
  })
  void narrowingCastRoundsBeforeCheckingPrecision(String sql) throws Exception {
    NativeParity.assertParity(
        () -> decimals(6, 3, "999.995", "-999.995", "999.994", "-999.994", "0.005", "-0.005", "0", null),
        sql);
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "SELECT id, CAST(id AS DECIMAL(1, 1)) FROM t",
    "SELECT id, CAST(id AS DECIMAL(38, 38)) IS NULL FROM t"
  })
  void integerCastOverflowProducesNull(String sql) throws Exception {
    NativeParity.assertParity(() -> decimals(1, 0, "0", "1", "-1"), sql);
  }

  private static TableEnvironment decimals(int precision, int scale, String... values) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    Row[] rows = new Row[values.length];
    for (int i = 0; i < values.length; i++) {
      rows[i] = Row.of((long) i, values[i] == null ? null : new BigDecimal(values[i]));
    }
    table.createTemporaryView(
        "t",
        env.fromData(Types.ROW_NAMED(new String[] {"id", "a"}, Types.LONG, Types.BIG_DEC), rows),
        Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("a", DataTypes.DECIMAL(precision, scale))
            .build());
    return table;
  }
}
