package tech.streamfusion;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Skips a test whose behaviour the Flink under test cannot exhibit yet.
 *
 * <p>Reserved for cases where the host itself lacks the behaviour, so there is no Flink result to
 * be identical to. A StreamFusion coverage gap must never be hidden behind this.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(FlinkVersionCondition.class)
@interface EnabledIfFlinkAtLeast {

  int major();

  int minor();

  /** Why the older line cannot run it, ideally the upstream issue key. */
  String reason();
}
