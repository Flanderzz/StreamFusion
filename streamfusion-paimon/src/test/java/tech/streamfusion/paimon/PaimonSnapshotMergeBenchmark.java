package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Includes Java merge-to-Arrow or native merge, file opening, planning, and Arrow import. */
class PaimonSnapshotMergeBenchmark {
  @ParameterizedTest
  @ValueSource(strings = {"int", "decimal", "timestamp", "binary", "date"})
  @EnabledIfEnvironmentVariable(named = "SF_PAIMON_SNAPSHOT_BENCHMARK", matches = "true")
  void compareSnapshotCatchup(String keyType) throws Exception {
    int rows = Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_SNAPSHOT_ROWS", "65536"));
    var type =
        new RowType(
            List.of(
                new DataField(
                    0,
                    "id",
                    switch (keyType) {
                      case "decimal" -> DataTypes.DECIMAL(38, 2).notNull();
                      case "timestamp" -> DataTypes.TIMESTAMP(6).notNull();
                      case "binary" -> DataTypes.VARBINARY(4).notNull();
                      case "date" -> DataTypes.DATE().notNull();
                      default -> DataTypes.INT().notNull();
                    }),
                new DataField(1, "v", DataTypes.STRING()),
                new DataField(2, "nested", DataTypes.ARRAY(DataTypes.INT())),
                new DataField(3, "ordinal", DataTypes.INT().notNull())));
    for (int runs : (keyType.equals("int") ? new int[] {1, 4, 8} : new int[] {4})) {
      var table =
          PaimonMergeEngineTest.table(
              Map.of("changelog-producer", "input", "write-only", "true"), type);
      var builder = table.newStreamWriteBuilder().withCommitUser("bench");
      try (var writer = builder.newWrite();
          var commit = builder.newCommit()) {
        for (int checkpoint = 1; checkpoint <= runs; checkpoint++) {
          for (int i = 0; i < rows; i++) {
            var row =
                GenericRow.of(
                    key(i - rows / 2, keyType),
                    BinaryString.fromString("value-" + checkpoint + "-" + i),
                    new GenericArray(new Integer[] {checkpoint, null, -i}),
                    i);
            if (checkpoint == runs && i % 7 == 0) {
              row.setRowKind(RowKind.DELETE);
            }
            writer.write(row);
          }
          commit.commit(checkpoint, writer.prepareCommit(true, checkpoint));
        }
      }
      var read = table.newReadBuilder();
      var splits = read.newStreamScan().plan().splits();
      double[] best = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
      for (int iteration = 0; iteration < 4; iteration++) {
        long[] expected = null;
        for (int offset = 0; offset < 2; offset++) {
          int nativeRead = (iteration + offset) % 2;
          long count = 0, checksum = 0;
          int merged = 0;
          long start = System.nanoTime();
          for (var split : splits) {
            try (var reader =
                new NativePaimonSplitReader(table, read, read.newRead(), 4096, -1)
                    .withNativeSnapshots(nativeRead == 1)) {
              reader.handleSplitsChanges(
                  new SplitsAddition<>(List.of(new FileStoreSourceSplit("split", split))));
              while (true) {
                var fetched = reader.fetch();
                if (fetched.nextSplit() != null) {
                  var record = fetched.nextRecordFromSplit();
                  if (record != null) {
                    try (var root = record.batch().root()) {
                      count += root.getRowCount();
                      var ids = (org.apache.arrow.vector.IntVector) root.getVector(3);
                      for (int row = 0; row < root.getRowCount(); row++) {
                        checksum += ids.get(row);
                      }
                    }
                  }
                }
                boolean finished = !fetched.finishedSplits().isEmpty();
                fetched.recycle();
                if (finished) {
                  break;
                }
              }
              merged += reader.nativeSnapshotsRead();
            }
          }
          double seconds = (System.nanoTime() - start) / 1e9;
          if (iteration > 0) {
            best[nativeRead] = Math.min(best[nativeRead], seconds);
          }
          if (nativeRead == 1) {
            assertTrue(merged > 0, "Must benchmark native merging");
          }
          if (expected == null) {
            expected = new long[] {count, checksum};
          } else {
            assertArrayEquals(expected, new long[] {count, checksum});
          }
        }
      }
      System.out.printf(
          "PAIMON_SNAPSHOT key=%s rows=%d commits=%d java_arrow_s=%.3f native_arrow_s=%.3f"
              + " speedup=%.2fx%n",
          keyType, rows, runs, best[0], best[1], best[0] / best[1]);
    }
  }

  private static Object key(int i, String type) {
    return switch (type) {
      case "decimal" ->
          org.apache.paimon.data.Decimal.fromBigDecimal(java.math.BigDecimal.valueOf(i, 2), 38, 2);
      case "timestamp" ->
          org.apache.paimon.data.Timestamp.fromEpochMillis(
              Math.floorDiv(i, 1000), Math.floorMod(i, 1000) * 1000);
      case "binary" -> java.nio.ByteBuffer.allocate(4).putInt(i).array();
      default -> i;
    };
  }
}
