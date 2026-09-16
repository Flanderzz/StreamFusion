package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.table.api.TableEnvironment;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

/** Independent executions: a host exception must never prevent the native-enabled run. */
final class NativeFailureParity {
  enum Phase { SETUP, PLANNING, SUBMISSION, INITIALIZATION, ROW_EVALUATION, COLLECTION }
  enum Route { HOST, NATIVE, FALLBACK, UNPLANNED }

  record Outcome(List<List<Object>> rows, Exception failure, Phase phase, Route route,
      List<String> fallbackReasons) {
    @Override
    public String toString() {
      return "Outcome[phase=" + phase + ", route=" + route + ", cause=" + rootCause()
          + ", collected=" + rows.size() + ", rows=" + rows.stream().limit(10).toList()
          + ", fallbackReasons=" + fallbackReasons + "]";
    }

    Throwable rootCause() {
      Throwable cause = failure;
      while (cause != null && cause.getCause() != null) cause = cause.getCause();
      return cause;
    }
  }

  record Comparison(Outcome host, Outcome nativeRun) {
    void assertFailure(Class<? extends Throwable> causeType, String message, Phase phase,
        Route nativeRoute) {
      assertEquals(host.failure() == null, nativeRun.failure() == null, this::toString);
      for (Outcome outcome : List.of(host, nativeRun)) {
        assertNotNull(outcome.failure(), outcome.toString());
        Throwable cause = outcome.rootCause();
        assertTrue(causeType.isInstance(cause), outcome.toString());
        assertTrue(String.valueOf(cause.getMessage()).contains(message), outcome.toString());
        assertEquals(phase, outcome.phase(), outcome.toString());
      }
      assertEquals(host.rootCause().getClass(), nativeRun.rootCause().getClass(), this::toString);
      assertEquals(nativeRoute, nativeRun.route(), this::toString);
    }

    void assertSuccess(Route nativeRoute) {
      assertEquals(null, host.failure(), this::toString);
      assertEquals(null, nativeRun.failure(), this::toString);
      assertEquals(sorted(host.rows()), sorted(nativeRun.rows()), this::toString);
      assertEquals(nativeRoute, nativeRun.route(), this::toString);
    }
  }

  static Comparison run(Supplier<TableEnvironment> environment, String sql) {
    Outcome host = execute(environment, sql, false);
    Outcome nativeRun = execute(environment, sql, true);
    return new Comparison(host, nativeRun);
  }

  private static Outcome execute(Supplier<TableEnvironment> environment, String sql, boolean nativeRun) {
    List<List<Object>> rows = new ArrayList<>();
    PhysicalPlanScan scan = null;
    Phase phase = Phase.SETUP;
    Exception failure = null;
    try {
      TableEnvironment table = environment.get();
      if (nativeRun) scan = NativePlanner.install(table);
      phase = Phase.PLANNING;
      var query = table.sqlQuery(sql);
      query.explain();
      phase = Phase.SUBMISSION;
      var result = query.execute();
      phase = Phase.COLLECTION;
      try (var iterator = result.collect()) {
        while (iterator.hasNext()) {
          var row = iterator.next();
          List<Object> fields = new ArrayList<>();
          fields.add(row.getKind().shortString());
          for (int i = 0; i < row.getArity(); i++) {
            fields.add(NativeParity.comparableValue(row.getField(i)));
          }
          rows.add(fields);
        }
      }
    } catch (Exception error) {
      failure = error;
      if (phase == Phase.SUBMISSION || phase == Phase.COLLECTION) {
        phase = failurePhase(error, phase);
      }
    }
    Route route = !nativeRun ? Route.HOST
        : scan == null || scan.operatorTypes().isEmpty() ? Route.UNPLANNED
        : scan.substitutions() > 0 ? Route.NATIVE : Route.FALLBACK;
    return new Outcome(rows, failure, phase, route,
        scan == null ? List.of() : List.copyOf(scan.fallbackReasons()));
  }

  /** Prefer the originating operator frame; the collect API also transports remote task failures. */
  private static Phase failurePhase(Throwable error, Phase observed) {
    List<Throwable> causes = new ArrayList<>();
    for (Throwable cause = error; cause != null; cause = cause.getCause()) causes.add(cause);
    for (int i = causes.size() - 1; i >= 0; i--) {
      for (StackTraceElement frame : causes.get(i).getStackTrace()) {
        String method = frame.getMethodName();
        if (method.equals("open") || method.equals("initializeState")) return Phase.INITIALIZATION;
        if (method.equals("processElement") || method.equals("processElement1")
            || method.equals("processElement2") || method.equals("endInput")
            || method.equals("eval") || method.equals("accumulate")) return Phase.ROW_EVALUATION;
      }
    }
    return observed;
  }

  private static List<List<Object>> sorted(List<List<Object>> rows) {
    List<List<Object>> copy = new ArrayList<>(rows);
    copy.sort(Comparator.comparing(Object::toString));
    return copy;
  }

  private NativeFailureParity() {}
}
