package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The registry write side the sink's open-time schema registration uses. */
class ConfluentSchemaRegistryTest {

  private static final String SCHEMA =
      "{\"type\":\"record\",\"name\":\"record\",\"namespace\":\"org.apache.flink.avro.generated\","
          + "\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";

  private HttpServer server;

  @AfterEach
  void stop() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void registersTheSchemaUnderItsSubjectAndReturnsTheId() throws Exception {
    AtomicReference<String> posted = new AtomicReference<>();
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/subjects/orders-value/versions",
        exchange -> {
          posted.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          byte[] body = "{\"id\":7}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();

    ConfluentSchemaRegistry registry =
        ConfluentSchemaRegistry.fromFormatOptions(Map.of("url", registryUrl()));
    assertNotNull(registry);
    assertEquals(7, registry.register("orders-value", SCHEMA));
    // The schema rides the versions POST as an escaped JSON string, the registry API's envelope.
    assertEquals(SCHEMA, new ObjectMapper().readTree(posted.get()).get("schema").asText());
  }

  /** An incompatible schema (the registry's 409) fails the job with the registry's message. */
  @Test
  void surfacesTheRegistrysRejection() throws Exception {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/subjects/orders-value/versions",
        exchange -> {
          byte[] body =
              "{\"error_code\":409,\"message\":\"Schema being registered is incompatible\"}"
                  .getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(409, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();

    ConfluentSchemaRegistry registry =
        ConfluentSchemaRegistry.fromFormatOptions(Map.of("url", registryUrl()));
    IOException failure =
        assertThrows(IOException.class, () -> registry.register("orders-value", SCHEMA));
    assertTrue(failure.getCause().getMessage().contains("incompatible"), failure::toString);
  }

  /** The sink-side option gate mirrors the decode side's untranslated-option fallbacks. */
  @Test
  void declinesUntranslatedRegistryOptions() {
    assertNull(ConfluentSchemaRegistry.fromFormatOptions(Map.of()));
    assertNull(
        ConfluentSchemaRegistry.fromFormatOptions(
            Map.of("url", "http://r:8081", "schema", "{\"type\":\"string\"}")));
    assertNull(
        ConfluentSchemaRegistry.fromFormatOptions(
            Map.of("url", "http://r:8081", "schema-registry.schema", "{\"type\":\"string\"}")));
    assertNull(
        ConfluentSchemaRegistry.fromFormatOptions(
            Map.of("url", "http://r:8081", "bearer-auth.token", "t")));
    assertNotNull(
        ConfluentSchemaRegistry.fromFormatOptions(Map.of("schema-registry.url", "http://r:8081")));
  }

  private String registryUrl() {
    return "http://localhost:" + server.getAddress().getPort();
  }
}
