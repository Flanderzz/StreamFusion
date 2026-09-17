package tech.streamfusion;

import java.time.ZoneId;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkFromUnixTimeSqlHarnessTest {
  private static final long[] EDGES = {
    0,
    -1,
    Long.MIN_VALUE,
    Long.MAX_VALUE,
    -12219292800L,
    -12219292801L,
    -62135769600L,
    -62135769601L,
    Long.MAX_VALUE / 1000,
    Long.MIN_VALUE / 1000,
    Long.MAX_VALUE / 1000 + 1,
    Long.MIN_VALUE / 1000 - 1,
    1710053999L,
    1710054000L,
    1730613599L,
    1730613600L,
    Integer.MIN_VALUE,
    Integer.MAX_VALUE
  };

  @ParameterizedTest
  @ValueSource(
      strings = {
        "UTC",
        "GMT+05:30",
        "GMT-12:00",
        "GMT+18:00",
        "Etc/GMT+8",
        "America/New_York",
        "Europe/Paris"
      })
  void fullRangeAndOverflowMatchFlinkInEveryAdmittedZone(String zone) throws Exception {
    NativeParity.assertParity(
        () -> environment(zone),
        "SELECT FROM_UNIXTIME(n), FROM_UNIXTIME(n, 'yyyyMMddHHmm'), "
            + "FROM_UNIXTIME(n, 'yyyy-MM-dd''T''HH:mm:ss''|😀'''), "
            + "FROM_UNIXTIME(i, 'yyyyMMddHHmm'), FROM_UNIXTIME(n, '') FROM src");
  }

  @Test
  void dynamicUnsupportedAndNullPatternsRemainHostExact() throws Exception {
    NativeParity.assertParity(
        () -> environment("UTC"),
        "SELECT FROM_UNIXTIME(n, p), FROM_UNIXTIME(n, 'EEEE MMMM dd yyyy'), "
            + "FROM_UNIXTIME(n, CAST(NULL AS STRING)), "
            + "CHAR_LENGTH(FROM_UNIXTIME(n, 'yyyyMMddHHmm')) FROM src");
  }

  @Test
  void nativeFormattingFeedsHostTemporalConsumers() throws Exception {
    NativeParity.assertParity(
        () -> environment("UTC"),
        "SELECT UNIX_TIMESTAMP(FROM_UNIXTIME(i)), FROM_UNIXTIME(n, 'yyyyMMddHHmm') IS NULL "
            + "FROM src");
  }

  @Test
  void invalidPatternKeepsFlinksFailure() {
    NativeFailureParity.run(() -> environment("UTC"), "SELECT FROM_UNIXTIME(n, 'yyyy-QQ') FROM src")
        .assertFailure(
            IllegalArgumentException.class,
            "Illegal pattern character 'Q'",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment environment(String zone) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.configure(
        org.apache.flink.configuration.Configuration.fromMap(
            java.util.Map.of("restart-strategy.type", "none")));
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneId.of(zone));
    table.createTemporaryView(
        "src",
        env.fromSequence(0, 5002)
            .map(
                id -> {
                  Long seconds;
                  if (id < EDGES.length) seconds = EDGES[id.intValue()];
                  else if (id % 8 == 0) seconds = null;
                  else seconds = new java.util.Random(id * 31 + 123).nextLong();
                  return Row.of(
                      seconds,
                      seconds == null ? null : seconds.intValue(),
                      id % 11 == 0 ? null : id % 2 == 0 ? "yyyyMMddHHmm" : "yyyy-MM-dd HH:mm:ss");
                })
            .returns(
                Types.ROW_NAMED(
                    new String[] {"n", "i", "p"}, Types.LONG, Types.INT, Types.STRING)));
    return table;
  }
}
