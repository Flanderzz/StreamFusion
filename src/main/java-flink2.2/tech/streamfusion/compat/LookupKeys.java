package tech.streamfusion.compat;

import java.util.Map;
import java.util.Set;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;

public record LookupKeys(Map<Integer, FunctionCallUtil.FunctionParam> values) {
  public Set<Integer> keySet() {
    return values.keySet();
  }
}
