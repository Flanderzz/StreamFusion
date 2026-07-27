package io.github.jordepic.streamfusion.state;

import io.github.jordepic.streamfusion.paimon.JavaPaimonStateCompactor;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;

/**
 * The Paimon-backend operator tests (checkpoint, restore, rescale) with the Java compactor
 * deployed: state tables carry deletion vectors, barriers compact synchronously, and a rescale
 * restore's clip rewrite is compacted before the first record — the paths the compactor-less
 * runtime-module run of this suite cannot reach.
 */
class DeletionVectorOperatorTest extends PaimonStateBackendOperatorTest {

  @BeforeAll
  static void requiresDeletionVectorCapableCompactor() {
    Assumptions.assumeTrue(
        new JavaPaimonStateCompactor().supportsDeletionVectors(),
        "deployed Paimon cannot compare binary primary-key lookup slices"
            + " (fix pending upstream); deletion-vector suites skip");
  }
}

