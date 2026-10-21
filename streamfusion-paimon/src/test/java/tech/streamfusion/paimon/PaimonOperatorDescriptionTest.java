package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class PaimonOperatorDescriptionTest {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void sourceAndSinkKeepTableAndFieldDetails(boolean simplify) {
    var execution = StreamExecutionEnvironment.getExecutionEnvironment();
    execution.setParallelism(1);
    var table = StreamTableEnvironment.create(execution);
    table.getConfig().set("table.exec.simplify-operator-name-enabled", "" + simplify);
    NativePlanner.install(table);
    table.executeSql(
        "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='" + directory.toUri() + "')");
    table.useCatalog("p");
    for (String name : List.of("input_rows", "output_rows")) {
      table.executeSql(
          "CREATE TABLE "
              + name
              + " (id INT, label STRING) WITH "
              + "('bucket'='-1', 'file.format'='parquet')");
    }
    String insert = "INSERT INTO output_rows SELECT * FROM input_rows";
    String plan = table.explainSql(insert);
    assertTrue(plan.contains("NativePaimonSource(table=[[p, default, input_rows]"), plan);
    assertTrue(plan.contains("NativePaimonSink(table=[p.default.output_rows]"), plan);
    assertTrue(plan.contains("fields=[id, label]"), plan);

    table.createStatementSet().addInsertSql(insert).attachAsDataStream();
    var graph = execution.getStreamGraph();
    for (String kind : List.of("NativeSource", "NativeSink")) {
      String name =
          graph.getStreamNodes().stream()
              .map(node -> node.getOperatorName())
              .filter(value -> value.contains(kind + "("))
              .findFirst()
              .orElseThrow();
      assertTrue(
          name.contains(
              kind.equals("NativeSource")
                  ? "table=[[p, default, input_rows]"
                  : "table=[p.default.output_rows]"),
          name);
      assertTrue(name.contains("fields=[id, label]"), name);
      assertFalse(name.contains("reuseBarrier"), name);
    }
  }
}
