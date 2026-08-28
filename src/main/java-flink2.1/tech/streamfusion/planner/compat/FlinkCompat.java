package tech.streamfusion.planner.compat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLookupJoin;
import org.apache.flink.table.planner.plan.abilities.source.WatermarkPushDownSpec;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtils;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.table.types.logical.RowType;

/**
 * The Flink 2.1 half of the planner seam. See the 2.2 copy for the contract; only the Flink-facing
 * names and the two capabilities 2.1 does not have differ.
 */
public final class FlinkCompat {

  private FlinkCompat() {}

  // ---------------------------------------------------------------- lookup join

  public static LookupKeys lookupKeys(StreamPhysicalLookupJoin join) {
    Map<Integer, FunctionCallUtils.FunctionParam> keys = new HashMap<>();
    scala.collection.JavaConverters.mapAsJavaMapConverter(join.allLookupKeys())
        .asJava()
        .forEach((index, param) -> keys.put((Integer) index, param));
    return LookupKeys.of(keys);
  }

  public static @Nullable String unsupportedKeyShape(LookupKeys keys) {
    for (Object param : keys.raw().values()) {
      if (!(param instanceof FunctionCallUtils.FieldRef)
          && !(param instanceof FunctionCallUtils.Constant)) {
        return "lookup join: unsupported lookup key shape " + param.getClass().getSimpleName();
      }
    }
    return null;
  }

  public static @Nullable AsyncLookupOptions asyncOptions(StreamPhysicalLookupJoin join) {
    if (join.asyncOptions().isEmpty()) {
      return null;
    }
    FunctionCallUtils.AsyncOptions options = join.asyncOptions().get();
    return new AsyncLookupOptions(options.asyncBufferCapacity, options.keyOrdered);
  }

  public static GeneratedAsyncFetcher generateAsyncFetcher(
      ReadableConfig config,
      ClassLoader classLoader,
      DataTypeFactory dataTypeFactory,
      RowType probeType,
      RowType tableSourceRowType,
      RowType resultRowType,
      LookupKeys lookupKeys,
      AsyncTableFunction<Object> lookupFunction,
      String tableName) {
    LookupJoinCodeGenerator.GeneratedTableFunctionWithDataType<AsyncFunction<RowData, Object>>
        generated =
            LookupJoinCodeGenerator.generateAsyncLookupFunction(
                config,
                classLoader,
                dataTypeFactory,
                probeType,
                tableSourceRowType,
                resultRowType,
                orderedKeys(lookupKeys),
                lookupFunction,
                tableName);
    return new GeneratedAsyncFetcher(generated.tableFunc(), generated.dataType());
  }

  public static GeneratedFunction<FlatMapFunction<RowData, RowData>> generateSyncFetcher(
      ReadableConfig config,
      ClassLoader classLoader,
      DataTypeFactory dataTypeFactory,
      RowType probeType,
      RowType tableSourceRowType,
      RowType resultRowType,
      LookupKeys lookupKeys,
      TableFunction<Object> lookupFunction,
      String tableName,
      boolean objectReuseEnabled) {
    return LookupJoinCodeGenerator.generateSyncLookupFunction(
        config,
        classLoader,
        dataTypeFactory,
        probeType,
        tableSourceRowType,
        resultRowType,
        orderedKeys(lookupKeys),
        lookupFunction,
        tableName,
        objectReuseEnabled);
  }

  public static Transformation<RowData> applyCustomShufflePartitioner(
      PlannerBase planner,
      RelOptTable temporalTable,
      RowType probeType,
      LookupKeys lookupKeys,
      Transformation<RowData> rows,
      ChangelogMode inputChangelogMode,
      TransformationMetadata metadata) {
    return LookupJoinUtil.tryApplyCustomShufflePartitioner(
        planner, temporalTable, probeType, rawKeys(lookupKeys), rows, inputChangelogMode, metadata);
  }

  private static List<FunctionCallUtils.FunctionParam> orderedKeys(LookupKeys lookupKeys) {
    Map<Integer, FunctionCallUtils.FunctionParam> keys = rawKeys(lookupKeys);
    List<FunctionCallUtils.FunctionParam> ordered = new ArrayList<>(keys.size());
    for (int key : LookupJoinUtil.getOrderedLookupKeys(keys.keySet())) {
      ordered.add(keys.get(key));
    }
    return ordered;
  }

  @SuppressWarnings("unchecked")
  private static Map<Integer, FunctionCallUtils.FunctionParam> rawKeys(LookupKeys lookupKeys) {
    return (Map<Integer, FunctionCallUtils.FunctionParam>) (Map<Integer, ?>) lookupKeys.raw();
  }

  // ------------------------------------------------------- changelog normalize

  /** Flink 2.1 has no source-reuse marking pass, so a normalize never shares a source. */
  public static boolean sharesSourceOrCommonFilter(StreamPhysicalChangelogNormalize normalize) {
    return false;
  }

  // ------------------------------------------------------------ watermark push-down

  /**
   * Flink 2.1 does not carry a rowtime expression on the pushed spec, and its watermark generator is
   * generated from the watermark expression alone, so there is nothing to cross-check against. The
   * caller reads the rowtime column out of the watermark expression either way.
   */
  public static Optional<RexNode> watermarkRowtimeExpr(WatermarkPushDownSpec spec) {
    return Optional.empty();
  }

  // ---------------------------------------------------------------- dimension calc

  /** Flink 2.1 returns the projection as a Scala {@code Seq}; the erased type is identical. */
  public static ExpandedCalc expandCalcProgram(RexProgram calc) {
    scala.Tuple2<scala.collection.Seq<RexNode>, scala.Option<RexNode>> expanded =
        FlinkRexUtil.expandRexProgram(calc);
    return new ExpandedCalc(
        scala.collection.JavaConverters.seqAsJavaListConverter(expanded._1()).asJava(),
        expanded._2().isDefined() ? expanded._2().get() : null);
  }

  // ----------------------------------------------------------------- state backend

  /**
   * Flink 2.1's keyed-state backend has no type identifier, so nothing on that line consumes this;
   * the value names the backend this one delegates to.
   */
  public static String backendTypeIdentifier(CheckpointableKeyedStateBackend<?> delegate) {
    return "rocksdb";
  }
}
