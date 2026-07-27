package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.paimon.JavaPaimonStateCompactor;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;

/**
 * The full Paimon-backend SQL parity suite with the Java compactor deployed: state tables carry
 * deletion vectors and compact synchronously at every barrier, so committed reads take the raw
 * parquet path with the vectors applied — the production shape. The same suite also runs
 * compactor-less in streamfusion-runtime, covering the degraded merge-read mode.
 */
class DeletionVectorSqlHarnessTest extends FlinkPaimonStateBackendSqlHarnessTest {

  @BeforeAll
  static void requiresDeletionVectorCapableCompactor() {
    Assumptions.assumeTrue(
        new JavaPaimonStateCompactor().supportsDeletionVectors(),
        "deployed Paimon cannot compare binary primary-key lookup slices"
            + " (fix pending upstream); deletion-vector suites skip");
  }
}

