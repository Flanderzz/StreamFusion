package tech.streamfusion.planner.compat;

import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.DataType;

/**
 * The generated async lookup fetcher and the data type its results arrive in.
 *
 * <p>Flink moved the class that pairs these two between 2.1 and 2.2; both members are unchanged, so
 * they are carried here instead and shared planner code never names the moved type.
 */
public final class GeneratedAsyncFetcher {

  private final GeneratedFunction<AsyncFunction<RowData, Object>> tableFunction;
  private final DataType dataType;

  public GeneratedAsyncFetcher(
      GeneratedFunction<AsyncFunction<RowData, Object>> tableFunction, DataType dataType) {
    this.tableFunction = tableFunction;
    this.dataType = dataType;
  }

  public GeneratedFunction<AsyncFunction<RowData, Object>> tableFunction() {
    return tableFunction;
  }

  public DataType dataType() {
    return dataType;
  }
}
