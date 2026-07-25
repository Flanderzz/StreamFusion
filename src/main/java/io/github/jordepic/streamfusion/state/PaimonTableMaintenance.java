package io.github.jordepic.streamfusion.state;

import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB-style background table maintenance. Compaction runs on a dedicated daemon thread per
 * keyed backend — never on the checkpoint barrier — kicked after every barrier's data commit,
 * since each barrier adds one sorted run per touched bucket, the analog of a memtable flush
 * adding an L0 file. Paimon's own strategy decides per bucket whether the run count warrants any
 * work ({@code num-sorted-run.compaction-trigger}, the analog of RocksDB's
 * {@code level0_file_num_compaction_trigger}), so an idle kick costs one plan scan and commits
 * nothing.
 *
 * <p>Maintenance is best-effort by design: a failed round is logged and retried on the next kick,
 * and never fails a checkpoint. A maintenance commit racing the barrier's data commit is resolved
 * by Paimon's optimistic commit retry on both sides; the store's local GC only ever deletes files
 * it previously listed as live, so a concurrent round's fresh outputs cannot be swept — at worst
 * an in-flight round loses an input file to GC, fails, and retries against the newer snapshot.
 */
final class PaimonTableMaintenance implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonTableMaintenance.class);

  private final StateTableCompactor compactor;
  private final File tableDirectory;
  private final long minIntervalMs;
  private final Thread thread;
  private final Object lock = new Object();
  private boolean kicked;
  private boolean closed;
  /** Monotonic across restarts (Paimon filters re-committed identifiers per commit user). */
  private long round = System.currentTimeMillis();

  PaimonTableMaintenance(StateTableCompactor compactor, File tableDirectory) {
    this.compactor = compactor;
    this.tableDirectory = tableDirectory;
    this.minIntervalMs =
        io.github.jordepic.streamfusion.planner.NativeConfig.paimonMaintenanceMinIntervalMs();
    this.thread = new Thread(this::run, "paimon-state-maintenance");
    this.thread.setDaemon(true);
    this.thread.start();
  }

  /** Signals that a barrier committed new sorted runs; coalesces while a round is in flight. */
  void kick() {
    synchronized (lock) {
      kicked = true;
      lock.notifyAll();
    }
  }

  private void run() {
    long lastRound = 0;
    while (true) {
      synchronized (lock) {
        while (!kicked && !closed) {
          try {
            lock.wait();
          } catch (InterruptedException e) {
            return;
          }
        }
        if (closed) {
          return;
        }
        // Pace the rounds: a round per barrier over-compacts at short checkpoint intervals
        // (the RocksDB analog compacts after several flushed runs, not after each). Kicks
        // arriving inside the pause coalesce into the round that follows it.
        long deadline = lastRound + minIntervalMs;
        long wait;
        while (!closed && (wait = deadline - System.currentTimeMillis()) > 0) {
          try {
            lock.wait(wait);
          } catch (InterruptedException e) {
            return;
          }
        }
        if (closed) {
          return;
        }
        kicked = false;
      }
      lastRound = System.currentTimeMillis();
      for (File table : PaimonSnapshotStrategy.discoverTables(tableDirectory)) {
        try {
          compactor.compact(table.getAbsolutePath(), ++round);
        } catch (Exception e) {
          LOG.warn(
              "state-table maintenance round failed; the next barrier retries it", e);
        }
      }
    }
  }

  @Override
  public void close() {
    synchronized (lock) {
      closed = true;
      lock.notifyAll();
    }
    // Let an in-flight round finish (its commit is safe either way) before resorting to an
    // interrupt; the table directory is deleted right after this returns.
    try {
      thread.join(10_000);
      if (thread.isAlive()) {
        thread.interrupt();
        thread.join(1_000);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
