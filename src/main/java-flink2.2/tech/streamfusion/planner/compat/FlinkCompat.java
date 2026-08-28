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
import org.apache.flink.table.planner.codegen.FunctionCallCodeGenerator;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLookupJoin;
import org.apache.flink.table.planner.plan.abilities.source.WatermarkPushDownSpec;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.table.types.logical.RowType;

/**
 * The Flink 2.2 half of the planner seam.
 *
 * <p>Everything Flink changed between the supported minor versions is reached through this class, so
 * the rest of the planner compiles once against a single source tree. Only the file is swapped per
 * Flink line — the signatures below are the contract both copies satisfy.
 */
public final class FlinkCompat {

  private FlinkCompat() {}

  // ---------------------------------------------------------------- lookup join

  /** Flink 2.2 renamed {@code FunctionCallUtils} to {@code FunctionCallUtil}; members are equal. */
  public static LookupKeys lookupKeys(StreamPhysicalLookupJoin join) {
    Map<Integer, FunctionCallUtil.FunctionParam> keys = new HashMap<>();
    scala.collection.JavaConverters.mapAsJavaMapConverter(join.allLookupKeys())
        .asJava()
        .forEach((index, param) -> keys.put((Integer) index, param));
    return LookupKeys.of(keys);
  }

  /** Returns a decline reason when any key is not a plain field reference or constant. */
  public static @Nullable String unsupportedKeyShape(LookupKeys keys) {
    for (Object param : keys.raw().values()) {
      if (!(param instanceof FunctionCallUtil.FieldRef)
          && !(param instanceof FunctionCallUtil.Constant)) {
        return "lookup join: unsupported lookup key shape " + param.getClass().getSimpleName();
      }
    }
    return null;
  }

  public static @Nullable AsyncLookupOptions asyncOptions(StreamPhysicalLookupJoin join) {
    if (join.asyncOptions().isEmpty()) {
      return null;
    }
    FunctionCallUtil.AsyncOptions options = join.asyncOptions().get();
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
    FunctionCallCodeGenerator.GeneratedTableFunctionWithDataType<AsyncFunction<RowData, Object>>
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

  /** The connector-owned partitioning SPI, which is typed on the renamed parameter map. */
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

  private static List<FunctionCallUtil.FunctionParam> orderedKeys(LookupKeys lookupKeys) {
    Map<Integer, FunctionCallUtil.FunctionParam> keys = rawKeys(lookupKeys);
    List<FunctionCallUtil.FunctionParam> ordered = new ArrayList<>(keys.size());
    for (int key : LookupJoinUtil.getOrderedLookupKeys(keys.keySet())) {
      ordered.add(keys.get(key));
    }
    return ordered;
  }

  @SuppressWarnings("unchecked")
  private static Map<Integer, FunctionCallUtil.FunctionParam> rawKeys(LookupKeys lookupKeys) {
    return (Map<Integer, FunctionCallUtil.FunctionParam>) (Map<Integer, ?>) lookupKeys.raw();
  }

  // ------------------------------------------------------- changelog normalize

  /**
   * Whether the rel carries the source-reuse marking Flink 2.2 added. The 2.2 optimizer runs a
   * {@code FlinkMarkChangelogNormalizeProgram} pass that can share one normalize across reused
   * sources and hoist a common filter; the native operator reproduces neither.
   */
  public static boolean sharesSourceOrCommonFilter(StreamPhysicalChangelogNormalize normalize) {
    return normalize.sourceReused() || normalize.commonFilter().length > 0;
  }

  // ------------------------------------------------------------ watermark push-down

  /**
   * The rowtime expression Flink 2.2 carries alongside the watermark expression. Flink 2.1 keeps
   * only the watermark expression, so the caller falls back to deriving the rowtime field itself.
   */
  public static Optional<RexNode> watermarkRowtimeExpr(WatermarkPushDownSpec spec) {
    return spec.getRowtimeExpr();
  }

  // ---------------------------------------------------------------- dimension calc

  /** Flink 2.2 returns the projection as a {@code java.util.List}. */
  public static ExpandedCalc expandCalcProgram(RexProgram calc) {
    scala.Tuple2<List<RexNode>, scala.Option<RexNode>> expanded =
        FlinkRexUtil.expandRexProgram(calc);
    return new ExpandedCalc(
        expanded._1(), expanded._2().isDefined() ? expanded._2().get() : null);
  }

  // ----------------------------------------------------------------- state backend

  /** Reported through the keyed-state backend interface from Flink 2.2 on. */
  public static String backendTypeIdentifier(CheckpointableKeyedStateBackend<?> delegate) {
    return delegate.getBackendTypeIdentifier();
  }
}
