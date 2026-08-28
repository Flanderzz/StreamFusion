package tech.streamfusion;

import java.util.Optional;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

class FlinkVersionCondition implements ExecutionCondition {

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    Optional<EnabledIfFlinkAtLeast> required =
        AnnotationSupport.findAnnotation(context.getElement(), EnabledIfFlinkAtLeast.class);
    if (required.isEmpty()) {
      return ConditionEvaluationResult.enabled("no Flink version requirement");
    }
    EnabledIfFlinkAtLeast annotation = required.get();
    if (HostFlinkVersion.atLeast(annotation.major(), annotation.minor())) {
      return ConditionEvaluationResult.enabled("host Flink is new enough");
    }
    return ConditionEvaluationResult.disabled(
        "needs Flink %d.%d or newer (%s); host is %s"
            .formatted(
                annotation.major(),
                annotation.minor(),
                annotation.reason(),
                HostFlinkVersion.current()));
  }
}
