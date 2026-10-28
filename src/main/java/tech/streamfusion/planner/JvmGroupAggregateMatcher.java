package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.utils.AggSqlFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.utils.ShortcutUtils;

final class JvmGroupAggregateMatcher {
  private JvmGroupAggregateMatcher() {}

  static boolean enabledAggregate(StreamPhysicalGroupAggregate node) {
    if (!Boolean.parseBoolean(
        System.getProperty("streamfusion.experimental.jvmGroupAggregates", "true"))) return false;
    for (int i = 0; i < node.aggCalls().size(); i++) {
      var call = node.aggCalls().apply(i);
      if (userFunction(call.getAggregation()) != null
          || call.getAggregation()
              == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.COLLECT
          || tech.streamfusion.compat.FlinkCompat.isBuiltinArrayAgg(call.getAggregation()))
        return true;
    }
    return false;
  }

  static boolean verifiedCollectionAggregate(
      StreamPhysicalGroupAggregate node, org.apache.calcite.rel.core.AggregateCall call) {
    boolean collect =
        call.getAggregation()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.COLLECT;
    if ((!collect && !tech.streamfusion.compat.FlinkCompat.isBuiltinArrayAgg(call.getAggregation()))
        || call.getArgList().size() != 1) return false;
    var value = node.getInput().getRowType().getFieldList().get(call.getArgList().get(0)).getType();
    return java.util.Set.of(
                org.apache.calcite.sql.type.SqlTypeName.BOOLEAN,
                org.apache.calcite.sql.type.SqlTypeName.TINYINT,
                org.apache.calcite.sql.type.SqlTypeName.SMALLINT,
                org.apache.calcite.sql.type.SqlTypeName.INTEGER,
                org.apache.calcite.sql.type.SqlTypeName.BIGINT,
                org.apache.calcite.sql.type.SqlTypeName.DATE,
                org.apache.calcite.sql.type.SqlTypeName.TIME)
            .contains(value.getSqlTypeName())
        && call.getType().getSqlTypeName()
            == (collect
                ? org.apache.calcite.sql.type.SqlTypeName.MULTISET
                : org.apache.calcite.sql.type.SqlTypeName.ARRAY)
        && call.getType().getComponentType().getSqlTypeName() == value.getSqlTypeName()
        && (value.isNullable()
            || !java.util.Set.of(
                    org.apache.calcite.sql.type.SqlTypeName.DATE,
                    org.apache.calcite.sql.type.SqlTypeName.TIME)
                .contains(value.getSqlTypeName())
            || !materializesTemporalHints(node.getInput()));
  }

  static boolean unverifiedTemporalCollectionInput(RelNode node) {
    if (node instanceof StreamPhysicalGroupAggregate) {
      var aggregate = (StreamPhysicalGroupAggregate) node;
      for (int i = 0; i < aggregate.aggCalls().size(); i++) {
        var call = aggregate.aggCalls().apply(i);
        if (call.getArgList().size() != 1
            || !(call.getAggregation()
                    == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.COLLECT
                || tech.streamfusion.compat.FlinkCompat.isBuiltinArrayAgg(call.getAggregation())))
          continue;
        var type =
            aggregate
                .getInput()
                .getRowType()
                .getFieldList()
                .get(call.getArgList().get(0))
                .getType();
        if (!type.isNullable()
            && java.util.Set.of(
                    org.apache.calcite.sql.type.SqlTypeName.DATE,
                    org.apache.calcite.sql.type.SqlTypeName.TIME)
                .contains(type.getSqlTypeName())
            && materializesTemporalHints(aggregate.getInput())) return true;
      }
    }
    return node.getInputs().stream()
        .anyMatch(JvmGroupAggregateMatcher::unverifiedTemporalCollectionInput);
  }

  // A released Calc can expose a NULL source row's hidden primitive payload under these
  // hints. Arrow validity retains the NULL, not that source-specific unchecked getter value.
  private static boolean materializesTemporalHints(RelNode node) {
    if (node instanceof org.apache.calcite.rel.core.Calc) {
      var calc = (org.apache.calcite.rel.core.Calc) node;
      var program = calc.getProgram();
      if (program.getCondition() != null
          && readsTemporalHint(program.expandLocalRef(program.getCondition()))) return true;
      boolean identity =
          program.getProjectList().size() == calc.getInput().getRowType().getFieldCount();
      for (int i = 0; identity && i < program.getProjectList().size(); i++) {
        var projection = program.expandLocalRef(program.getProjectList().get(i));
        identity =
            projection instanceof org.apache.calcite.rex.RexInputRef
                && ((org.apache.calcite.rex.RexInputRef) projection).getIndex() == i;
      }
      if (!identity
          && program.getProjectList().stream()
              .map(program::expandLocalRef)
              .anyMatch(JvmGroupAggregateMatcher::readsTemporalHint)) return true;
    }
    return node.getInputs().stream().anyMatch(JvmGroupAggregateMatcher::materializesTemporalHints);
  }

  private static boolean readsTemporalHint(org.apache.calcite.rex.RexNode expression) {
    boolean[] found = {false};
    expression.accept(
        new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
          @Override
          public Void visitInputRef(org.apache.calcite.rex.RexInputRef input) {
            var type = input.getType();
            if (!type.isNullable()
                && java.util.Set.of(
                        org.apache.calcite.sql.type.SqlTypeName.DATE,
                        org.apache.calcite.sql.type.SqlTypeName.TIME)
                    .contains(type.getSqlTypeName())) found[0] = true;
            return null;
          }
        });
    return found[0];
  }

  private static AggregateFunction<?, ?> userFunction(SqlAggFunction function) {
    Object definition =
        function instanceof BridgingSqlAggFunction
            ? ((BridgingSqlAggFunction) function).getDefinition()
            : function.getClass() == AggSqlFunction.class
                ? ((AggSqlFunction) function).aggregateFunction()
                : null;
    return definition instanceof AggregateFunction ? (AggregateFunction<?, ?>) definition : null;
  }

  static boolean matches(StreamPhysicalGroupAggregate node) {
    var config = ShortcutUtils.unwrapTableConfig(node);
    if (config.get(
            org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
        || config.get(
            org.apache.flink.configuration.ConfigOptions.key("table.exec.async-state.enabled")
                .booleanType()
                .defaultValue(false))) return false;
    if (!JvmFunctionTypes.scalarRow(node.getInput().getRowType())) return false;
    for (int i = 0; i < node.aggCalls().size(); i++) {
      var call = node.aggCalls().apply(i);
      if (!JvmFunctionTypes.scalar(call.getType()) && !verifiedCollectionAggregate(node, call))
        return false;
      if (verifiedCollectionAggregate(node, call)
          && node.getRowType().getFieldList().stream()
              .skip(node.grouping().length)
              .anyMatch(field -> JsonStringIdentity.containsCharacter(field.getType())))
        return false;
      if (call.isApproximate() || !call.getCollation().getFieldCollations().isEmpty()) return false;
      var aggregation = call.getAggregation();
      if (aggregation instanceof AggSqlFunction && aggregation.getClass() != AggSqlFunction.class)
        return false;
      var function = userFunction(aggregation);
      if (function instanceof org.apache.flink.table.functions.python.PythonFunction
          || function instanceof SpecializedFunction) return false;
      if (aggregation instanceof BridgingSqlAggFunction
          && ((BridgingSqlAggFunction) aggregation).getDefinition() instanceof SpecializedFunction)
        return false;
    }
    return true;
  }

  static String unsupportedReason(StreamPhysicalGroupAggregate node) {
    return "GROUP BY: JVM aggregates require verified scalar fields or verified COLLECT/ARRAY_AGG"
        + " with non-character aggregate results and synchronous single-phase execution without"
        + " mini-batching, specialization, approximate or ordered calls";
  }

  static RelNode substitute(StreamPhysicalGroupAggregate node, PlanContext context) {
    int[] keys = GroupAggregateMatcher.keyColumns(node);
    return new StreamPhysicalNativeJvmGroup(
        node.getCluster(),
        node.getTraitSet(),
        context.columnarInput(node.getInput(), keys),
        node.getRowType(),
        keys,
        node.aggCalls(),
        AggregateUtil.deriveAggCallNeedRetractions(node, keys.length, node.aggCalls()),
        !ChangelogPlanUtils.inputInsertOnly(node),
        ChangelogPlanUtils.generateUpdateBefore(node),
        tech.streamfusion.compat.FlinkCompat.singleStateTtl(node));
  }
}
