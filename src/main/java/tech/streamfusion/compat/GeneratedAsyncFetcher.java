package tech.streamfusion.compat;

import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.DataType;

public record GeneratedAsyncFetcher(
    GeneratedFunction<AsyncFunction<RowData, Object>> tableFunc, DataType dataType) {}
