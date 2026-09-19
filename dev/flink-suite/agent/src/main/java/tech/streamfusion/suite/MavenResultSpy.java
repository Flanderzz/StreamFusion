package tech.streamfusion.suite;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.maven.eventspy.AbstractEventSpy;
import org.apache.maven.execution.ExecutionEvent;
import org.apache.maven.execution.MavenExecutionResult;
import org.apache.maven.lifecycle.LifecycleExecutionException;
import org.apache.maven.plugin.MojoFailureException;

/** Records a completed Maven session independently of the available Surefire XML reports. */
public final class MavenResultSpy extends AbstractEventSpy {
  private final List<Throwable> testFailures = new ArrayList<>();

  @Override
  public void onEvent(Object event) throws Exception {
    String output = System.getProperty("streamfusion.flink-suite.maven-result");
    if (output == null) return;
    if (event instanceof ExecutionEvent execution
        && execution.getType() == ExecutionEvent.Type.MojoFailed) {
      var mojo = execution.getMojoExecution();
      Throwable failure = execution.getException();
      if (failure instanceof LifecycleExecutionException) failure = failure.getCause();
      // Surefire 3.2.2 reports assertion failures without a cause. Fork exceptions,
      // internal errors and timeouts use a different exception, cause or message.
      if (mojo != null
          && "org.apache.maven.plugins".equals(mojo.getGroupId())
          && "maven-surefire-plugin".equals(mojo.getArtifactId())
          && "3.2.2".equals(mojo.getVersion())
          && "test".equals(mojo.getGoal())
          && failure != null
          && failure.getClass() == MojoFailureException.class
          && failure.getCause() == null
          && failure.getMessage() != null
          && failure.getMessage().startsWith("There are test failures.")) {
        testFailures.add(failure);
      }
    } else if (event instanceof MavenExecutionResult result) {
      String status =
          !result.hasExceptions()
              ? "success"
              : result.getExceptions().stream().allMatch(this::isObservedTestFailure)
                  ? "test-failures"
                  : "failure";
      Files.writeString(Path.of(output), "streamfusion-maven-result-v1\t" + status + "\n");
    }
  }

  private boolean isObservedTestFailure(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (testFailures.contains(cause)) return true;
    }
    return false;
  }
}
