package tech.streamfusion.compat;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

/** Keep columnar operators in the upstream task chain when Flink's topology permits it. */
public abstract class FlinkStreamOperator<OUT> extends AbstractStreamOperator<OUT> {
  private static final long serialVersionUID = 1L;

  protected FlinkStreamOperator() {}
}
