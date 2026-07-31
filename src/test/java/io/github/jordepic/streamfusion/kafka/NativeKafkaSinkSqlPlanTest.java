package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-kafka")
class NativeKafkaSinkSqlPlanTest {

  @Test
  void plansNativeSerializationWithStockExactlyOnceKafka() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT, name STRING, ts TIMESTAMP(3)) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, name STRING, ts TIMESTAMP(3)) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'json', "
            + "'sink.delivery-guarantee' = 'exactly-once', "
            + "'sink.transactional-id-prefix' = 'streamfusion-test')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
    assertTrue(plan.contains("native-kafka-exactly-once-sink"), plan);
  }

  @Test
  void plansTheVerifiedScalarJsonFamily() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (amount DECIMAL(10, 2), payload BYTES, event_day DATE, tod TIME(3), "
            + "instant TIMESTAMP_LTZ(3)) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (amount DECIMAL(10, 2), payload BYTES, event_day DATE, tod TIME(3), "
            + "instant TIMESTAMP_LTZ(3)) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'json')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    // Without exactly-once, the sink keeps the encode-only shape feeding Flink's own KafkaSink.
    assertFalse(plan.contains("native-kafka-exactly-once-sink"), plan);
  }

  @Test
  void plansNestedRowsAndArraysNatively() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id INT, items ARRAY<INT>, "
            + "nested ROW<a INT, ts TIMESTAMP_LTZ(3), inner_items ARRAY<ROW<b STRING>>>) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id INT, items ARRAY<INT>, "
            + "nested ROW<a INT, ts TIMESTAMP_LTZ(3), inner_items ARRAY<ROW<b STRING>>>) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'json')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
  }

  @Test
  void plansStringKeyedMapsAndMultisetsNatively() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id INT, counts MAP<STRING, INT>, bag MULTISET<STRING>, "
            + "deep MAP<STRING, ARRAY<ROW<a INT>>>) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id INT, counts MAP<STRING, INT>, bag MULTISET<STRING>, "
            + "deep MAP<STRING, ARRAY<ROW<a INT>>>) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'json', "
            + "'json.map-null-key.mode' = 'LITERAL', "
            + "'json.map-null-key.literal' = 'absent')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT * FROM src", ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
  }

  /**
   * Flink's own JSON converter rejects a non-string map key when the sink translates; the native
   * matcher must decline first so substituting the sink never swallows that rejection.
   */
  @Test
  void keepsFlinksRejectionOfNonStringMapKeys() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id INT, counts MAP<INT, STRING>) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '1')");
    table.executeSql(
        "CREATE TABLE output (id INT, counts MAP<INT, STRING>) WITH ("
            + "'connector' = 'kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'format' = 'json')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    UnsupportedOperationException rejection =
        assertThrows(
            UnsupportedOperationException.class,
            () -> table.explainSql("INSERT INTO output SELECT * FROM src"));

    assertTrue(rejection.getMessage().contains("non-string as key type"), rejection.getMessage());
    assertEquals(0, scan.substitutions());
    assertTrue(
        scan.fallbackReasons().stream().anyMatch(reason -> reason.contains("MAP")),
        scan::explainSummary);
  }

  @Test
  void plansUpdatingResultsThroughNativeUpsertSerialization() {
    StreamTableEnvironment table = environment();
    table.executeSql(
        "CREATE TABLE src (id BIGINT) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '10')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, total BIGINT, PRIMARY KEY (id) NOT ENFORCED) WITH ("
            + "'connector' = 'upsert-kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'key.format' = 'json', "
            + "'value.format' = 'json', "
            + "'sink.delivery-guarantee' = 'exactly-once', "
            + "'sink.transactional-id-prefix' = 'streamfusion-upsert-test')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT id, COUNT(*) FROM src GROUP BY id",
            ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
    assertTrue(plan.contains("NativeKafkaSink"), plan);
    assertTrue(plan.contains("native-kafka-exactly-once-sink"), plan);
  }

  @Test
  void upsertMaterializedSinkKeepsHostSerialization() {
    // When Flink materializes an out-of-order upsert changelog (SinkUpsertMaterializer), the
    // materializer is baked into its sink translation — substituting the sink would drop it, so the
    // matcher must decline. FORCE makes the materialization deterministic for the test.
    StreamTableEnvironment table = environment();
    table.getConfig().set("table.exec.sink.upsert-materialize", "FORCE");
    table.executeSql(
        "CREATE TABLE src (id BIGINT) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '10')");
    table.executeSql(
        "CREATE TABLE output (id BIGINT, total BIGINT, PRIMARY KEY (id) NOT ENFORCED) WITH ("
            + "'connector' = 'upsert-kafka', "
            + "'topic' = 'output', "
            + "'properties.bootstrap.servers' = 'broker:9092', "
            + "'key.format' = 'json', "
            + "'value.format' = 'json')");

    PhysicalPlanScan scan = NativePlanner.install(table);
    String plan =
        table.explainSql(
            "INSERT INTO output SELECT id, COUNT(*) FROM src GROUP BY id",
            ExplainDetail.JSON_EXECUTION_PLAN);

    assertFalse(plan.contains("NativeKafkaSink"), plan);
    assertTrue(plan.contains("SinkMaterializer"), plan);
    assertTrue(
        scan.fallbackReasons().stream()
            .anyMatch(reason -> reason.contains("upsert-materialized sink")),
        scan::explainSummary);
  }

  private static StreamTableEnvironment environment() {
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.setParallelism(1);
    return StreamTableEnvironment.create(environment);
  }
}
