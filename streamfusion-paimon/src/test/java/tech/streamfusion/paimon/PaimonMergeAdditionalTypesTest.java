package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class PaimonMergeAdditionalTypesTest {
  @ParameterizedTest
  @ValueSource(strings = {"deduplicate", "partial-update", "aggregation"})
  void defaultsOnPrimaryKeysRequireStockRouting(String engine) throws Exception {
    var schema =
        new RowType(
            List.of(
                new DataField(0, "id", DataTypes.INT().notNull(), null, "7"),
                new DataField(1, "v", DataTypes.INT())));
    var table = PaimonMergeEngineTest.table(Map.of("merge-engine", engine), schema);
    assertEquals(
        "default values on routing columns require the stock Paimon writer",
        PaimonMergeOptions.unsupportedReason(table));
  }

  static Stream<Arguments> floatingModes() {
    return Stream.of("min", "max", "sequence", "descending", "sequence-group")
        .flatMap(mode -> Stream.of(Arguments.of(mode, false), Arguments.of(mode, true)));
  }

  @ParameterizedTest
  @MethodSource("floatingModes")
  void floatingOrderMatchesJavaIncludingNanPayloadsAndSignedZero(String mode, boolean single)
      throws Exception {
    var type = single ? DataTypes.FLOAT() : DataTypes.DOUBLE();
    var schema =
        new RowType(
            List.of(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "v", type),
                new DataField(2, "tag", DataTypes.STRING())));
    List<InternalRow> rows = new ArrayList<>();
    double[] values = {
      -0.0,
      0.0,
      Double.NEGATIVE_INFINITY,
      Double.POSITIVE_INFINITY,
      Double.NaN,
      Double.longBitsToDouble(0xfff8000000000001L)
    };
    for (int key = 0; key < 3; key++) {
      rows.add(GenericRow.of(key, null, BinaryString.fromString("initial")));
      for (int i = 0; i < (key == 0 ? 2 : key == 1 ? 4 : values.length); i++) {
        Object value;
        if (single) value = Float.valueOf((float) values[i]);
        else value = Double.valueOf(values[i]);
        rows.add(GenericRow.of(key, value, BinaryString.fromString("value-" + i)));
      }
    }
    compare(schema, orderingOptions(mode), rows);
  }

  private static Map<String, String> orderingOptions(String mode) {
    Map<String, String> options = new HashMap<>();
    if (mode.equals("sequence-group")) {
      options.put("merge-engine", "partial-update");
      options.put("fields.v.sequence-group", "tag");
    } else if (mode.equals("sequence") || mode.equals("descending")) {
      options.put("sequence.field", "v");
      options.put(
          "sequence.field.sort-order", mode.equals("descending") ? "descending" : "ascending");
    } else {
      options.put("merge-engine", "aggregation");
      options.put("fields.v.aggregate-function", mode);
    }
    return options;
  }

  @ParameterizedTest
  @ValueSource(strings = {"|", "::", ""})
  void distinctConcatenationKeepsExistingDuplicatesAndIgnoresBlankTokens(String delimiter)
      throws Exception {
    var schema =
        new RowType(
            List.of(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "v", DataTypes.STRING())));
    String split = delimiter.isEmpty() ? " " : delimiter;
    List<InternalRow> rows =
        List.of(
            GenericRow.of(1, null),
            GenericRow.of(1, BinaryString.fromString("a" + split + "a")),
            GenericRow.of(1, BinaryString.fromString("a" + split + "b" + split + "b")),
            GenericRow.of(1, BinaryString.fromString(split + " " + split + "c")),
            GenericRow.of(2, BinaryString.fromString("single" + split + "single")),
            GenericRow.of(3, BinaryString.fromString("\u2003")),
            GenericRow.of(3, BinaryString.fromString("\u00a0")));
    compare(
        schema,
        Map.of(
            "merge-engine",
            "aggregation",
            "fields.v.aggregate-function",
            "listagg",
            "fields.v.distinct",
            "true",
            "fields.v.list-agg-delimiter",
            delimiter),
        rows);
  }

  private static void compare(RowType schema, Map<String, String> options, List<InternalRow> rows)
      throws Exception {
    var table = PaimonMergeEngineTest.table(options, schema);
    var stock = PaimonMergeEngineTest.table(options, schema);
    assertNull(PaimonMergeOptions.unsupportedReason(table));
    for (int run = 1; run <= 2; run++) {
      try (var ours =
              new PaimonMergeEngineTest.Writer(
                  table, true, new PaimonChangelogSinkWriteTest.MemoryState(), 3);
          var twin =
              new PaimonMergeEngineTest.Writer(
                  stock, false, new PaimonChangelogSinkWriteTest.MemoryState(), 3)) {
        ours.write(rows);
        twin.write(rows);
        ours.commit(run);
        twin.commit(run);
      }
      assertEquals(
          PaimonTestTables.readRows(stock, schema),
          PaimonTestTables.readRows(table, schema),
          "checkpoint " + run);
    }
  }
}
