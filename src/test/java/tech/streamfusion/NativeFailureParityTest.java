package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.JobCancellationException;
import org.junit.jupiter.api.Test;

class NativeFailureParityTest {
  @Test
  void terminalOperatorErrorTakesPrecedenceOverACollectorTransportRace() {
    ArithmeticException operatorFailure = new ArithmeticException("/ by zero");
    IOException collectorFailure = new IOException("Task is not running, but in state FAILED");
    CompletableFuture<?> job = CompletableFuture.failedFuture(operatorFailure);

    Exception failure = NativeFailureParity.terminalFailure(job, collectorFailure);

    assertInstanceOf(ExecutionException.class, failure);
    assertSame(operatorFailure, failure.getCause());
  }

  @Test
  void successfulJobDoesNotHideALocalCollectorFailure() {
    IOException collectorFailure = new IOException("invalid collected row");

    Exception failure =
        NativeFailureParity.terminalFailure(
            CompletableFuture.completedFuture(null), collectorFailure);

    assertSame(collectorFailure, failure);
  }

  @Test
  void iteratorCleanupCancellationDoesNotReplaceTheOriginalFailure() {
    IOException collectorFailure = new IOException("invalid collected row");
    var cancelled = new JobCancellationException(new JobID(), "iterator closed", null);

    Exception failure =
        NativeFailureParity.terminalFailure(
            CompletableFuture.failedFuture(cancelled), collectorFailure);

    assertSame(collectorFailure, failure);
  }

  @Test
  void interruptedFailureLookupPreservesTheInterruptAndOriginalFailure() {
    IOException collectorFailure = new IOException("collect interrupted");
    Thread.currentThread().interrupt();
    try {
      Exception failure =
          NativeFailureParity.terminalFailure(new CompletableFuture<>(), collectorFailure);

      assertSame(collectorFailure, failure);
      assertTrue(Thread.currentThread().isInterrupted());
      assertInstanceOf(InterruptedException.class, failure.getSuppressed()[0]);
    } finally {
      Thread.interrupted();
    }
  }
}
