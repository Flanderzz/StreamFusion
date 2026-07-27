package io.github.jordepic.streamfusion.state;

/**
 * The maintainer of a native operator's Paimon state table, discovered via {@link
 * java.util.ServiceLoader}. The native store itself never compacts: whatever maintenance happens
 * comes from an implementation of this interface, run at every checkpoint barrier on the task
 * thread, immediately before the store commits its write buffer, so the maintenance snapshot
 * always lands directly beneath the checkpoint's data snapshot. Without one, state tables stay
 * correct but accumulate one sorted run per touched bucket per checkpoint.
 *
 * <p>The shipped implementation ({@code streamfusion-paimon-compactor}) delegates to stock Java
 * Paimon: its own compaction picks, its sequence-preserving rewriter, its deletion handling.
 */
public interface StateTableCompactor {

  /** Whether this compactor's dependencies are on the classpath (e.g. a Paimon bundle). */
  boolean available();

  /**
   * Whether this compactor can maintain tables of the given data file format. A compactor that
   * cannot read the format must decline — the tables then run unmaintained (e.g. Java Paimon
   * releases before 2.0 have no vortex format factory).
   */
  boolean supports(String fileFormat);

  /**
   * Whether this compactor can maintain deletion-vector tables: their lookup compaction compares
   * lookup-file keys through the deployed Paimon's slice comparator, which older releases break
   * on binary primary-key fields (ClassCastException — fixed upstream by comparing binary fields
   * like BinaryRow does). When false, new state tables are created without deletion vectors and
   * reads merge sorted runs; maintenance still runs at every barrier.
   */
  default boolean supportsDeletionVectors() {
    return false;
  }

  /**
   * The minimal maintenance a barrier must wait for: up-level the barrier's level-0 runs (with
   * deletion vectors maintained) and nothing else. Deletion-vector reads skip level 0, so this
   * is correctness-critical and runs synchronously inside the snapshot; everything
   * discretionary — merging level-1+ runs for read and space amplification — belongs to {@link
   * #shape} off the barrier path. On a deletion-vector table a failure fails the snapshot; on a
   * merge-read table the caller may log and continue.
   *
   * @param tableDirectory the state table's local directory
   * @param round a monotonic commit identifier
   */
  void compact(String tableDirectory, long round) throws Exception;

  /**
   * One discretionary shaping round: ordinary compaction picks (universal triggers) bounding run
   * counts and space amplification. Runs on a background thread; deletion vectors keep reads
   * correct however far shaping lags, so a failed round is only a lost optimization.
   */
  default void shape(String tableDirectory, long round) throws Exception {}
}
