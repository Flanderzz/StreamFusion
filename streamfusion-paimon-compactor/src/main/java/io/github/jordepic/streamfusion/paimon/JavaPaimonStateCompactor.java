package io.github.jordepic.streamfusion.paimon;

import io.github.jordepic.streamfusion.state.StateTableCompactor;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.apache.paimon.CoreOptions;
import java.util.List;
import java.util.Comparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;

/**
 * Table maintenance by stock Java Paimon: each checkpoint opens the local state table, asks
 * Paimon's own compaction to look at every live bucket ({@code fullCompaction=false}, so its
 * universal strategy picks — usually nothing), and commits whatever it rewrote as a maintenance
 * snapshot directly beneath the checkpoint's data commit. Sequence numbers are preserved by
 * Paimon's rewriter and deletions drop exactly per its own rules.
 */
public class JavaPaimonStateCompactor implements StateTableCompactor {

  private static final String COMMIT_USER = "streamfusion-compactor";
  private static final String SHAPE_COMMIT_USER = "streamfusion-shaper";

  @Override
  public boolean available() {
    try {
      Class.forName("org.apache.paimon.table.FileStoreTableFactory");
      return true;
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      return false;
    }
  }

  @Override
  public boolean supports(String fileFormat) {
    // The deployed Paimon must have a reader/writer for the state files (vortex arrives with
    // Paimon 2.0; parquet is always in the bundle).
    try {
      org.apache.paimon.factories.FormatFactoryUtil.discoverFactory(
          JavaPaimonStateCompactor.class.getClassLoader(), fileFormat.toLowerCase());
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }

  /**
   * Probes the deployed Paimon's slice comparator with the state tables' exact key shape
   * (INT key group, VARBINARY key): releases without the binary-field fix throw
   * ClassCastException the first time lookup compaction seeks a lookup file, so a broken
   * deployment must fall back to merge-read tables up front rather than fail at the first
   * post-restore barrier.
   */
  @Override
  public boolean supportsDeletionVectors() {
    try {
      RowCompactedSerializer serializer =
          new RowCompactedSerializer(RowType.of(DataTypes.INT(), DataTypes.BYTES()));
      Comparator<MemorySlice> comparator = serializer.createSliceComparator();
      MemorySlice small =
          MemorySlice.wrap(serializer.serializeToBytes(GenericRow.of(1, new byte[] {1})));
      MemorySlice large =
          MemorySlice.wrap(serializer.serializeToBytes(GenericRow.of(1, new byte[] {2})));
      return comparator.compare(small, large) < 0 && comparator.compare(large, small) > 0;
    } catch (RuntimeException | Error probeFailure) {
      return false;
    }
  }

  @Override
  public void compact(String tableDirectory, long round) throws Exception {
    run(tableDirectory, round, COMMIT_USER, true);
  }

  @Override
  public void shape(String tableDirectory, long round) throws Exception {
    // Ordinary universal picks under the table's own triggers. A distinct commit user keeps the
    // identifier sequence independent of the barrier rounds (Paimon dedupes per user).
    run(tableDirectory, round, SHAPE_COMMIT_USER, false);
  }

  private static void run(String tableDirectory, long round, String commitUser, boolean minimal)
      throws Exception {
    FileStoreTable table =
        FileStoreTableFactory.create(LocalFileIO.create(), new Path(tableDirectory));
    if (table.snapshotManager().latestSnapshotId() == null) {
      return; // nothing committed yet
    }
    if (minimal) {
      // The barrier waits on this round, so disable every discretionary pick: with the
      // universal triggers unreachable, ForceUpLevel0Compaction falls through to exactly the
      // minimal correctness-critical rewrite — up-level the barrier's level-0 runs, marking
      // overwritten rows in higher levels through the lookup index instead of merging them.
      // num-levels must be pinned to the table's real value first: its default is derived from
      // the run trigger, and deriving it from MAX_VALUE would ask Levels for two billion runs.
      Map<String, String> options = new HashMap<>();
      options.put(CoreOptions.NUM_LEVELS.key(), String.valueOf(table.coreOptions().numLevels()));
      options.put(
          CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(),
          String.valueOf(Integer.MAX_VALUE));
      options.put(
          CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT.key(),
          String.valueOf(Integer.MAX_VALUE));
      table = table.copy(options);
    }
    // Ask compaction to look at every bucket by the table's fixed bucket count — a read plan
    // cannot discover them, because scans of a deletion-vector table skip level-0 files, which
    // is exactly where the runs needing compaction sit. An untouched bucket costs one empty
    // strategy pick.
    int buckets = table.coreOptions().bucket();
    if (buckets <= 0) {
      return;
    }
    StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(commitUser);
    // Lookup compaction (the deletion-vector rewriter) spills its key-position lookup files
    // through an IOManager; give it scratch space under the JVM temp dir.
    try (IOManager ioManager =
            IOManager.create(
                Files.createTempDirectory("streamfusion-compactor-lookup").toString());
        StreamTableWrite write = builder.newWrite();
        StreamTableCommit commit = builder.newCommit()) {
      write.withIOManager(ioManager);
      for (int bucket = 0; bucket < buckets; bucket++) {
        write.compact(BinaryRow.EMPTY_ROW, bucket, false);
      }
      List<CommitMessage> messages = write.prepareCommit(true, round);
      // Nothing picked -> no snapshot; an empty maintenance commit every barrier would bloat
      // snapshot history for no work.
      boolean empty =
          messages.stream().allMatch(message -> ((CommitMessageImpl) message).isEmpty());
      if (!empty) {
        commit.commit(round, messages);
      }
    }
  }
}
