package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class PaimonSourceSqlTest {
  @Test
  void sourceUidMatchesPaimonAcrossGraphChanges() throws Exception {
    List<List<String>> twins = new ArrayList<>();
    for (boolean nativeSource : new boolean[] {false, true}) {
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      var sql = StreamTableEnvironment.create(env);
      sql.executeSql(
          "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='"
              + Files.createTempDirectory("paimon-source-uid").toUri()
              + "')");
      sql.executeSql("USE CATALOG p");
      sql.executeSql("CREATE TABLE t (id INT) WITH ('bucket'='-1', 'file.format'='parquet')");
      if (nativeSource) {
        NativePlanner.install(sql);
      }
      List<String> uids = new ArrayList<>();
      for (String suffix : List.of("before", "after")) {
        var stream =
            sql.toDataStream(
                sql.sqlQuery(
                    "SELECT * FROM t /*+ OPTIONS('source.operator-uid.suffix'='"
                        + suffix
                        + "') */"));
        var sources =
            stream.getTransformation().getTransitivePredecessors().stream()
                .filter(
                    t ->
                        t
                            instanceof
                            org.apache.flink.streaming.api.transformations.SourceTransformation)
                .toList();
        assertEquals(1, sources.size());
        assertNotNull(sources.get(0).getUid());
        if (nativeSource) {
          assertEquals("native-paimon-source", sources.get(0).getName());
        }
        uids.add(sources.get(0).getUid());
      }
      assertNotEquals(uids.get(0), uids.get(1));
      twins.add(uids);
    }
    assertEquals(twins.get(0), twins.get(1));
  }

  @ParameterizedTest
  @CsvSource({
    "false,false,INT",
    "true,false,INT",
    "false,true,INT",
    "true,true,INT",
    "true,false,'DECIMAL(38, 2)'",
    "true,false,DATE",
    "true,false,TIMESTAMP(6)",
    "true,false,VARBINARY(4)"
  })
  void streamingSqlReadsSnapshotThenNewCommit(boolean primaryKey, boolean watermark, String keyType)
      throws Exception {
    List<List<String>> twins = new ArrayList<>();
    for (boolean nativeSource : new boolean[] {false, true}) {
      var warehouse = Files.createTempDirectory("paimon-source-sql");
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(2);
      env.enableCheckpointing(100);
      var sql = StreamTableEnvironment.create(env);
      sql.executeSql(
          "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='" + warehouse.toUri() + "')");
      sql.executeSql("USE CATALOG p");
      sql.executeSql(
          "CREATE TABLE t (id "
              + keyType
              + " NOT NULL, v STRING, pt STRING NOT NULL, ts TIMESTAMP(3)"
              + (watermark ? ", WATERMARK FOR ts AS ts - INTERVAL '1' SECOND" : "")
              + (primaryKey ? ", PRIMARY KEY (id, pt) NOT ENFORCED" : "")
              + ") PARTITIONED BY (pt) WITH ('bucket'='"
              + (primaryKey ? "2" : "-1")
              + "', 'file.format'='parquet', 'changelog-producer'='"
              + (primaryKey ? "input" : "none")
              + "', 'write-only'='true', 'continuous.discovery-interval'='10 ms')");
      FileStoreTable table =
          FileStoreTableFactory.create(
              LocalFileIO.create(), new Path(warehouse.resolve("default.db/t").toUri()));
      var builder = table.newStreamWriteBuilder().withCommitUser("test");
      try (var writer = builder.newWrite();
          var commit = builder.newCommit()) {
        for (int i = 0; i < 3; i++) {
          writer.write(
              GenericRow.of(
                  sqlKey(i, keyType),
                  BinaryString.fromString("before"),
                  BinaryString.fromString("p"),
                  org.apache.paimon.data.Timestamp.fromEpochMillis(i * 2000L)));
        }
        commit.commit(1, writer.prepareCommit(true, 1));
        if (primaryKey) {
          for (int i = 0; i < 3; i++) {
            writer.write(
                GenericRow.of(
                    sqlKey(i, keyType),
                    BinaryString.fromString("merged"),
                    BinaryString.fromString("p"),
                    org.apache.paimon.data.Timestamp.fromEpochMillis(i * 2000L)));
          }
          commit.commit(2, writer.prepareCommit(true, 2));
        }
        var plan = nativeSource ? NativePlanner.install(sql) : null;
        var result = sql.executeSql("SELECT ts, pt, id, v FROM t WHERE v IS NOT NULL");
        var executor = Executors.newSingleThreadExecutor();
        try (var rows = result.collect()) {
          CountDownLatch initial = new CountDownLatch(1);
          var collected =
              executor.submit(
                  () -> {
                    List<String> values = new ArrayList<>();
                    while (values.size() < 6 && rows.hasNext()) {
                      values.add(rows.next().toString());
                      if (values.size() == 3) {
                        initial.countDown();
                      }
                    }
                    values.sort(String::compareTo);
                    return values;
                  });
          try {
            assertTrue(initial.await(45, TimeUnit.SECONDS), "initial snapshot timed out");
            for (int i = 3; i < 6; i++) {
              writer.write(
                  GenericRow.of(
                      sqlKey(i, keyType),
                      BinaryString.fromString("after"),
                      BinaryString.fromString("p"),
                      org.apache.paimon.data.Timestamp.fromEpochMillis(i * 2000L)));
            }
            int checkpoint = primaryKey ? 3 : 2;
            commit.commit(checkpoint, writer.prepareCommit(true, checkpoint));
            twins.add(collected.get(45, TimeUnit.SECONDS));
            if (plan != null) {
              assertTrue(plan.substitutions() > 0, plan.explainSummary());
              assertTrue(
                  plan.fallbackReasons().stream().noneMatch(r -> r.startsWith("Paimon source:")),
                  plan.explainSummary());
            }
          } finally {
            result.getJobClient().orElseThrow().cancel().get(30, TimeUnit.SECONDS);
            executor.shutdownNow();
          }
        }
      }
    }
    assertEquals(twins.get(0), twins.get(1));
  }

  private static Object sqlKey(int i, String type) {
    return switch (type) {
      case "DECIMAL(38, 2)" ->
          org.apache.paimon.data.Decimal.fromBigDecimal(
              java.math.BigDecimal.valueOf(i - 3, 2), 38, 2);
      case "DATE" -> i - 3;
      case "TIMESTAMP(6)" -> org.apache.paimon.data.Timestamp.fromEpochMillis(-1, i * 1000);
      case "VARBINARY(4)" -> new byte[] {(byte) (i + 125)};
      default -> i;
    };
  }
}
