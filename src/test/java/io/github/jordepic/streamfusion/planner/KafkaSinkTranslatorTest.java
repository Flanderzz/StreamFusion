package io.github.jordepic.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.junit.jupiter.api.Test;

class KafkaSinkTranslatorTest {

  @Test
  void preservesTheStockExactlyOnceWriterContract() {
    KafkaSinkTranslator.Result result =
        KafkaSinkTranslator.translate(
            Map.of(
                "connector", "kafka",
                "topic", "output",
                "properties.bootstrap.servers", "broker:9092",
                "properties.compression.type", "lz4",
                "format", "json",
                "sink.delivery-guarantee", "exactly-once",
                "sink.transactional-id-prefix", "orders",
                "sink.parallelism", "3"));

    assertTrue(result.fallbackReason == null, () -> result.fallbackReason);
    assertEquals(DeliveryGuarantee.EXACTLY_ONCE, result.planned().deliveryGuarantee);
    assertEquals("orders", result.planned().transactionalIdPrefix);
    assertEquals("lz4", result.planned().producerProperties.getProperty("compression.type"));
    assertEquals(
        "lz4", result.planned().nativeProducerConfig.nativeConfig().get("compression.type"));
    assertEquals(3, result.planned().parallelism);
  }

  @Test
  void declinesShapesWhoseRowwiseSemanticsAreNotYetModeled() {
    Map<String, String> base =
        Map.of(
            "topic", "output",
            "properties.bootstrap.servers", "broker:9092",
            "format", "json");
    assertFallback(with(base, "key.format", "json"), "key format");
    assertFallback(with(base, "sink.partitioner", "fixed"), "partitioner");
    assertFallback(with(base, "sink.buffer-flush.max-rows", "10"), "buffer");
    assertFallback(with(base, "topic", "a;b"), "one fixed topic");
    assertFallback(with(base, "format", "avro"), "not yet natively encoded");
  }

  @Test
  void collectsKeyAndValueFormatOptionsSeparately() {
    KafkaSinkTranslator.Result result =
        KafkaSinkTranslator.translate(
            Map.of(
                "connector", "upsert-kafka",
                "topic", "output",
                "properties.bootstrap.servers", "broker:9092",
                "key.format", "json",
                "value.format", "json",
                "key.json.timestamp-format.standard", "ISO-8601",
                "value.json.encode.decimal-as-plain-number", "true"));

    assertTrue(result.fallbackReason == null, () -> result.fallbackReason);
    assertEquals(
        Map.of("timestamp-format.standard", "ISO-8601"), result.planned().keyJsonOptions);
    assertEquals(
        Map.of("encode.decimal-as-plain-number", "true"), result.planned().jsonOptions);
  }

  @Test
  void requiresAStableTransactionalPrefixForExactlyOnce() {
    KafkaSinkTranslator.Result result =
        KafkaSinkTranslator.translate(
            Map.of(
                "topic", "output",
                "properties.bootstrap.servers", "broker:9092",
                "format", "json",
                "sink.delivery-guarantee", "exactly-once"));
    assertTrue(result.fallbackReason != null);
    assertTrue(result.fallbackReason.contains("transactional-id-prefix"));
  }

  @Test
  void fallsBackWhenAProducerPropertyCannotRunNatively() {
    KafkaSinkTranslator.Result result =
        KafkaSinkTranslator.translate(
            Map.of(
                "topic", "output",
                "properties.bootstrap.servers", "broker:9092",
                "properties.interceptor.classes", "com.example.AuditInterceptor",
                "format", "json",
                "sink.delivery-guarantee", "exactly-once",
                "sink.transactional-id-prefix", "orders"));
    assertTrue(result.fallbackReason != null);
    assertTrue(result.fallbackReason.contains("interceptor.classes"));
  }

  private static void assertFallback(Map<String, String> options, String expected) {
    KafkaSinkTranslator.Result result = KafkaSinkTranslator.translate(options);
    assertTrue(result.fallbackReason != null);
    assertTrue(result.fallbackReason.contains(expected));
  }

  private static Map<String, String> with(Map<String, String> base, String key, String value) {
    java.util.HashMap<String, String> copy = new java.util.HashMap<>(base);
    copy.put(key, value);
    return copy;
  }
}
