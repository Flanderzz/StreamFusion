package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

class FlussPlanAssertionsTest {
  @Test
  void countsSharedSourcesByTheirRuntimeTypeWithADifferentPlanDescription() {
    var graph =
        graph("Source: NativeSource(table=[source], fields=[id])", "NativeShareExecNode[1]");
    ((ObjectNode) graph.get("nodes").get(0))
        .put("contents", "NativeFlussSource(table=[source], fields=[id])");
    FlussPlanAssertions.assertSingleNativeSource(graph, "q3");
    assertThrows(
        AssertionError.class,
        () ->
            FlussPlanAssertions.assertSingleNativeSource(
                graph(
                    "Source: NativeSource(table=[source], fields=[id])",
                    "Source: NativeSource(table=[source], fields=[id])"),
                "q3"));
    assertThrows(
        AssertionError.class,
        () -> FlussPlanAssertions.assertSingleNativeSource(graph("Source: events"), "q3"));
  }

  @Test
  void permitsTimestampInsertionOnlyAfterThePrimaryKeySinkTranspose() throws Exception {
    var graph =
        graph(
            "Source: NativeSource(table=[source], fields=[id])",
            "NativeCalc(select=[id], nativeFunctions=[], jvmFunctions=[])[1]",
            "ArrowToRowDataExecNode[2]",
            "ConstraintEnforcer[3]",
            "StreamRecordTimestampInserter[4]",
            "Sink(result): Writer");
    FlussPlanAssertions.assertNativeInterior(graph, true, "q18");
    assertThrows(
        AssertionError.class,
        () -> FlussPlanAssertions.assertNativeInterior(graph, false, "append"));
  }

  @Test
  void rejectsStockOperatorsInTheQueryInterior() throws Exception {
    var graph =
        graph(
            "Source: NativeSource(table=[source], fields=[id])",
            "StreamRecordTimestampInserter[1]",
            "NativeDeduplicateExecNode[2]",
            "ArrowToRowDataExecNode[3]",
            "Sink(result): Writer");
    assertThrows(
        AssertionError.class, () -> FlussPlanAssertions.assertNativeInterior(graph, true, "q18"));
    var stockAggregate =
        graph(
            "Source: NativeSource(table=[source], fields=[id])",
            "GroupAggregate[1]",
            "NativeSink(table=[target], fields=[id]): Writer");
    assertThrows(
        AssertionError.class,
        () -> FlussPlanAssertions.assertNativeInterior(stockAggregate, false, "append"));
  }

  private static com.fasterxml.jackson.databind.JsonNode graph(String... types) {
    var mapper = new ObjectMapper();
    var result = mapper.createObjectNode();
    var nodes = result.putArray("nodes");
    for (int i = 0; i < types.length; i++) {
      var node = nodes.addObject().put("id", i).put("type", types[i]);
      if (i > 0) node.putArray("predecessors").addObject().put("id", i - 1);
    }
    return result;
  }
}
