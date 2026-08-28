package tech.streamfusion.planner.compat;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The dimension-key → probe field/constant map a lookup join builds its key row from, carried
 * opaquely.
 *
 * <p>Flink renamed the enclosing utility (and therefore the parameter type) between 2.1 and 2.2
 * without changing any member. Shared planner code passes this holder around and never names the
 * Flink type; {@code FlinkCompat} — the one class compiled per Flink line — is the only place that
 * unwraps it.
 */
public final class LookupKeys {

  private final Map<Integer, Object> byIndex;

  private LookupKeys(Map<Integer, Object> byIndex) {
    this.byIndex = Collections.unmodifiableMap(byIndex);
  }

  /** Wraps Flink's already-extracted parameter map. Called only from {@code FlinkCompat}. */
  public static LookupKeys of(Map<Integer, ?> byIndex) {
    return new LookupKeys(new LinkedHashMap<>(byIndex));
  }

  public Set<Integer> indexes() {
    return byIndex.keySet();
  }

  public int size() {
    return byIndex.size();
  }

  /** The raw Flink parameters. Callers outside {@code FlinkCompat} must treat these as opaque. */
  public Map<Integer, Object> raw() {
    return byIndex;
  }
}
