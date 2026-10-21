package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;

final class FlussPlanAssertions {
  private FlussPlanAssertions() {}

  static void assertSingleNativeSource(JsonNode execution, String query) {
    assertEquals(
        1,
        execution.get("nodes").findValuesAsText("type").stream()
            .filter(type -> type.startsWith("Source: NativeSource("))
            .count(),
        query + " must share one columnar source between both join branches");
  }

  static void assertNativeInterior(JsonNode execution, boolean primaryKeySink, String query) {
    Map<Integer, JsonNode> nodes = new HashMap<>();
    for (JsonNode node : execution.get("nodes")) nodes.put(node.get("id").asInt(), node);
    for (JsonNode node : nodes.values()) {
      String operator = node.get("type").asText();
      if (isNative(operator)) continue;
      assertTrue(
          primaryKeySink && isSinkBoundary(operator),
          query + " unexpected stock interior operator: " + operator);
      JsonNode current = node;
      int remaining = nodes.size();
      while (!current.get("type").asText().startsWith("ArrowToRowData")) {
        assertTrue(remaining-- > 0, query + " cyclic sink boundary");
        assertTrue(
            isSinkBoundary(current.get("type").asText()),
            query + " stock operator is outside the primary-key sink boundary");
        assertEquals(1, current.path("predecessors").size(), query + " sink boundary input");
        current = nodes.get(current.get("predecessors").get(0).get("id").asInt());
        assertTrue(current != null, query + " missing sink boundary predecessor");
      }
      assertEquals(1, current.path("predecessors").size(), query + " transpose input");
      JsonNode predecessor = nodes.get(current.get("predecessors").get(0).get("id").asInt());
      assertTrue(
          predecessor != null && isNative(predecessor.get("type").asText()),
          query + " transpose must consume a native operator");
    }
  }

  private static boolean isNative(String type) {
    return type.startsWith("Native") || type.startsWith("Source: NativeSource(");
  }

  private static boolean isSinkBoundary(String type) {
    return type.startsWith("ArrowToRowData")
        || type.startsWith("Sink(")
        || type.startsWith("ConstraintEnforcer[")
        || type.startsWith("StreamRecordTimestampInserter[");
  }
}
