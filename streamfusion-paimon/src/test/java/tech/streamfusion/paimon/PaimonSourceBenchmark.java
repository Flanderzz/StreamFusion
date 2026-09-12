package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Committed file read, Parquet decode, and projected-column checksum; release builds only. */
class PaimonSourceBenchmark {
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PAIMON_SOURCE_BENCHMARK", matches = "true")
  void compareReleasedReader() throws Exception {
    int rows = Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_SOURCE_ROWS", "262144"));
    for (boolean primaryKey : new boolean[] {false, true}) {
      FileStoreTable table =
          primaryKey
              ? PaimonMergeEngineTest.table(
                  Map.of(
                      "changelog-producer", "input", "write-only", "true", "scan.mode", "latest"))
              : PaimonTestTables.createTable(
                  Files.createTempDirectory("paimon-source-bench"),
                  Map.of("bucket", "-1", "file.format", "parquet"));
      if (primaryKey) {
        try (var initial =
            new PaimonMergeEngineTest.Writer(
                table, false, new PaimonChangelogSinkWriteTest.MemoryState(), 7)) {
          initial.write(PaimonMergeEngineTest.rows(1, false));
          initial.commit(1);
        }
      }
      var read =
          table
              .newReadBuilder()
              .withProjection(primaryKey ? new int[] {0, 3, 4, 7} : new int[] {0, 1, 2, 7});
      var scan = read.newStreamScan();
      if (primaryKey) {
        scan.plan();
      }
      var builder = table.newStreamWriteBuilder().withCommitUser("bench");
      try (var writer = builder.newWrite();
          var commit = builder.newCommit()) {
        if (primaryKey) {
          for (var row : PaimonMergeEngineTest.rows(rows, false)) {
            writer.write(row);
          }
        } else {
          for (var row : PaimonTestTables.values(rows)) {
            writer.write(PaimonTestTables.paimonRow(row));
          }
        }
        commit.commit(2, writer.prepareCommit(true, 2));
      }
      var splits = scan.plan().splits();
      double[] best = {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
      for (int run = 0; run < 4; run++) {
        Long expected = null;
        for (int offset = 0; offset < 2; offset++) {
          int nativeRead = (offset + run) % 2;
          long checksum = 0;
          long count = 0;
          long start = System.nanoTime();
          for (int i = 0; i < splits.size(); i++) {
            if (nativeRead == 0) {
              try (var reader = read.newRead().createReader(splits.get(i))) {
                org.apache.paimon.reader.RecordReader.RecordIterator<
                        org.apache.paimon.data.InternalRow>
                    batch;
                while ((batch = reader.readBatch()) != null) {
                  org.apache.paimon.data.InternalRow row;
                  while ((row = batch.next()) != null) {
                    count++;
                    checksum += primaryKey ? row.getInt(0) : row.getLong(0);
                  }
                  batch.releaseBatch();
                }
              }
            } else {
              try (var reader =
                  new NativePaimonSplitReader(table, read, read.newRead(), 4096, -1)) {
                reader.handleSplitsChanges(
                    new SplitsAddition<>(
                        List.of(new FileStoreSourceSplit("file-" + i, splits.get(i)))));
                boolean finished = false;
                while (!finished) {
                  var fetched = reader.fetch();
                  if (fetched.nextSplit() != null) {
                    var record = fetched.nextRecordFromSplit();
                    if (record != null) {
                      try (var root = record.batch().root()) {
                        count += root.getRowCount();
                        for (int r = 0; r < root.getRowCount(); r++) {
                          checksum +=
                              primaryKey
                                  ? ((org.apache.arrow.vector.IntVector) root.getVector(0)).get(r)
                                  : ((org.apache.arrow.vector.BigIntVector) root.getVector(0))
                                      .get(r);
                        }
                      }
                    }
                  }
                  finished = !fetched.finishedSplits().isEmpty();
                  fetched.recycle();
                }
                assertTrue(reader.nativeFilesRead() > 0);
              }
            }
          }
          double seconds = (System.nanoTime() - start) / 1e9;
          if (run > 0) {
            best[nativeRead] = Math.min(best[nativeRead], seconds);
          }
          assertEquals(rows, count);
          if (expected == null) {
            expected = checksum;
          } else {
            assertEquals(expected.longValue(), checksum);
          }
        }
      }
      System.out.printf(
          "PAIMON_SOURCE mode=%s rows=%d stock_s=%.3f native_s=%.3f speedup=%.2fx%n",
          primaryKey ? "changelog" : "append", rows, best[0], best[1], best[0] / best[1]);
    }
  }
}
