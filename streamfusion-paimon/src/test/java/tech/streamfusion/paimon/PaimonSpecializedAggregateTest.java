package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static tech.streamfusion.paimon.PaimonMergeEngineTest.options;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.HllSketchUtil;
import org.apache.paimon.utils.RoaringBitmap32;
import org.apache.paimon.utils.RoaringBitmap64;
import org.apache.paimon.utils.ThetaSketch;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class PaimonSpecializedAggregateTest {
  static Stream<Map<String, String>> collections() {
    return Stream.of(false, true)
        .flatMap(
            distinct ->
                Stream.of(false, true)
                    .map(
                        partial -> {
                          Map<String, String> options =
                              options(
                                  "merge-engine",
                                  partial ? "partial-update" : "aggregation",
                                  "fields.nested.aggregate-function",
                                  "collect",
                                  "fields.nested.distinct",
                                  distinct.toString());
                          if (partial) {
                            options.put(
                                "fields.seq,seq2.sequence-group", "v,txt,flag,amount,nested");
                          }
                          return options;
                        }));
  }

  @ParameterizedTest
  @MethodSource("collections")
  void collectionKernelsMatchReleasedWriterAcrossRetractsAndRestart(Map<String, String> options)
      throws Exception {
    assertWriterParity(options, PaimonMergeEngineTest.TYPE, PaimonMergeEngineTest.rows(240, false));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "merge_map",
        "merge_map_with_keytime",
        "nested_update",
        "nested_partial_update",
        "collect_rows",
        "hll_sketch",
        "theta_sketch",
        "rbm32",
        "rbm64"
      })
  void specializedKernelsPreserveNestedValuesAndSerializedSketches(String function)
      throws Exception {
    RowType nested =
        new RowType(
            List.of(
                new DataField(3, "nk", DataTypes.INT()),
                new DataField(4, "txt", DataTypes.STRING()),
                new DataField(5, "ts", DataTypes.STRING())));
    DataType valueType =
        switch (function) {
          case "merge_map" -> DataTypes.MAP(DataTypes.INT(), DataTypes.STRING());
          case "merge_map_with_keytime" -> DataTypes.MAP(DataTypes.INT(), nested);
          case "nested_update", "nested_partial_update", "collect_rows" -> DataTypes.ARRAY(nested);
          default -> DataTypes.BYTES();
        };
    RowType type =
        new RowType(
            List.of(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "seq", DataTypes.BIGINT()),
                new DataField(2, "v", valueType)));
    Map<String, String> mode =
        options(
            "merge-engine", "aggregation",
            "fields.v.aggregate-function", function.equals("collect_rows") ? "collect" : function,
            "fields.v.distinct", "true");
    if (function.startsWith("nested_")) mode.put("fields.v.nested-key", "nk");
    if (function.equals("nested_update")) mode.put("fields.v.nested-sequence-field", "ts");
    List<InternalRow> input = new ArrayList<>();
    for (int i = 0; i < 240; i++) {
      BinaryString text = i % 7 == 0 ? null : BinaryString.fromString("v" + i);
      var child = GenericRow.of(i % 3, text, BinaryString.fromString("t" + i % 7));
      Map<Integer, Object> map = new HashMap<>();
      map.put(i % 3, function.equals("merge_map") ? text : child);
      map.put(9, null);
      Object value =
          switch (function) {
            case "merge_map", "merge_map_with_keytime" -> new GenericMap(map);
            case "collect_rows" -> new GenericArray(new InternalRow[] {child, child});
            case "nested_update", "nested_partial_update" ->
                new GenericArray(new InternalRow[] {child, null, child});
            case "hll_sketch" -> HllSketchUtil.sketchOf(i % 17, i % 11);
            case "theta_sketch" -> ThetaSketch.sketchOf(i % 17, i % 11);
            case "rbm32" -> RoaringBitmap32.bitmapOf(i % 17, i % 11).serialize();
            case "rbm64" -> RoaringBitmap64.bitmapOf((1L << 40) + i % 17, i % 11).serialize();
            default -> throw new AssertionError(function);
          };
      input.add(GenericRow.of(i % 7, (long) i, i % 13 == 0 ? null : value));
    }
    assertWriterParity(mode, type, input);
  }

  private static void assertWriterParity(
      Map<String, String> options, RowType type, List<InternalRow> input) throws Exception {
    var stock = PaimonMergeEngineTest.table(options, type);
    var nativeTable = PaimonMergeEngineTest.table(options, type);
    assertNull(PaimonMergeOptions.unsupportedReason(nativeTable));
    var stockState = new PaimonChangelogSinkWriteTest.MemoryState();
    var nativeState = new PaimonChangelogSinkWriteTest.MemoryState();
    for (int run = 0; run < 2; run++) {
      try (var expected = new PaimonMergeEngineTest.Writer(stock, false, stockState, 7);
          var actual = new PaimonMergeEngineTest.Writer(nativeTable, true, nativeState, 7)) {
        for (int checkpoint = run * 2 + 1; checkpoint <= run * 2 + 2; checkpoint++) {
          var rows = input.subList((checkpoint - 1) * 60, checkpoint * 60);
          expected.write(rows);
          actual.write(rows);
          expected.commit(checkpoint);
          actual.commit(checkpoint);
          assertEquals(
              PaimonTestTables.readRows(stock, type),
              PaimonTestTables.readRows(nativeTable, type),
              "checkpoint " + checkpoint);
        }
      }
    }
  }
}
