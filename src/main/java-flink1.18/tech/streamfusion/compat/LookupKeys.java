package tech.streamfusion.compat;

import java.util.Map;
import java.util.Set;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;

public record LookupKeys(Map<Integer, LookupJoinUtil.LookupKey> values) {
  public Set<Integer> keySet() {
    return values.keySet();
  }
}
