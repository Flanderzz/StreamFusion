package tech.streamfusion;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.compat.FlinkTestSources;

class FlinkStateBackendAdmissionTest {
  private static final String GROUP = "SELECT k, SUM(v) FROM t GROUP BY k";
  private static final String REASON =
      "state backend: Flink 1.18 native keyed state requires heap state or the StreamFusion RocksDB"
          + " backend";

  @Test
  void explicitStockRocksBackendDeclinesKeyedSqlBeforeExecution() throws Exception {
    NativeParity.assertFallbackReasonContains(() -> environment(true, false), GROUP, REASON);
  }

  @Test
  void configuredStockRocksBackendDeclinesKeyedSqlBeforeExecution() throws Exception {
    NativeParity.assertFallbackReasonContains(() -> environment(false, true), GROUP, REASON);
  }

  @Test
  void stockRocksBackendStillAdmitsStatelessSql() throws Exception {
    NativeParity.assertParity(() -> environment(true, false), "SELECT v + 1 FROM t");
  }

  @Test
  void heapBackendStillAdmitsKeyedSql() throws Exception {
    NativeParity.assertChangelogParity(() -> environment(false, false), GROUP);
  }

  private static TableEnvironment environment(boolean explicitRocks, boolean configuredRocks) {
    Configuration configuration = new Configuration();
    if (configuredRocks) configuration.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(1);
    if (explicitRocks) env.setStateBackend(new EmbeddedRocksDBStateBackend());
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "t",
        FlinkTestSources.fromData(env, Row.of(1L, 10L), Row.of(1L, 5L), Row.of(2L, 7L))
            .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG)));
    return table;
  }
}
