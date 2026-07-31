package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.format.EncodeFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/** Conservative match boundary for native JSON serialization into Flink's Kafka sink. */
final class KafkaSinkMatcher {

  private KafkaSinkMatcher() {}

  static final class Planned {
    final RowType rowType;
    final KafkaSinkTranslator.Planned sink;
    final EncodeFormat valueFormat;
    final EncodeFormat keyFormat;
    final int[] keyFields;
    final int[] valueFields;
    final boolean upsert;
    final String fallbackReason;

    private Planned(
        RowType rowType,
        KafkaSinkTranslator.Planned sink,
        EncodeFormat valueFormat,
        EncodeFormat keyFormat,
        int[] keyFields,
        int[] valueFields,
        boolean upsert,
        String fallbackReason) {
      this.rowType = rowType;
      this.sink = sink;
      this.valueFormat = valueFormat;
      this.keyFormat = keyFormat;
      this.keyFields = keyFields;
      this.valueFields = valueFields;
      this.upsert = upsert;
      this.fallbackReason = fallbackReason;
    }

    private static Planned fallback(String reason) {
      return new Planned(null, null, null, null, null, null, false, reason);
    }
  }

  static boolean appliesTo(StreamPhysicalSink sink) {
    Map<String, String> options = options(sink);
    if (options == null
        || !("kafka".equals(options.get("connector"))
            || "upsert-kafka".equals(options.get("connector")))) {
      return false;
    }
    return "json".equals(options.getOrDefault("value.format", options.get("format")));
  }

  static Planned plan(StreamPhysicalSink sink) {
    // Flink materializes an out-of-order upsert changelog with a SinkUpsertMaterializer (a stateful
    // operator baked into its sink translation); substituting the sink would silently drop it.
    if (sink.upsertMaterialize()) {
      return Planned.fallback(
          "an upsert-materialized sink (SinkUpsertMaterializer) is not natively reproduced");
    }
    if (sink.abilitySpecs().length != 0) {
      SinkAbilitySpec spec = sink.abilitySpecs()[0];
      return Planned.fallback("sink ability " + spec.getClass().getSimpleName());
    }
    KafkaSinkTranslator.Result translated = KafkaSinkTranslator.translate(options(sink));
    if (translated.fallbackReason != null) {
      return Planned.fallback(translated.fallbackReason);
    }
    ContextResolvedTable context = sink.contextResolvedTable();
    ResolvedCatalogTable table = (ResolvedCatalogTable) context.getResolvedTable();
    RowType rowType =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    for (LogicalType type : rowType.getChildren()) {
      if (!supportsJsonType(type)) {
        return Planned.fallback("JSON type " + type.asSummaryString());
      }
    }
    EncodeFormat valueFormat =
        EncodeFormat.of(translated.planned().valueFormat, translated.planned().valueFormatOptions);
    if (valueFormat == null) {
      return Planned.fallback(
          "value format " + translated.planned().valueFormat + " is not natively encoded"
              + " with these options");
    }
    EncodeFormat keyFormat = valueFormat;
    if (translated.planned().upsert) {
      keyFormat =
          EncodeFormat.of(translated.planned().keyFormat, translated.planned().keyFormatOptions);
      if (keyFormat == null) {
        return Planned.fallback(
            "key format " + translated.planned().keyFormat + " is not natively encoded"
                + " with these options");
      }
    }
    int[] valueFields = IntStream.range(0, rowType.getFieldCount()).toArray();
    int[] keyFields = new int[0];
    if (translated.planned().upsert) {
      List<String> primaryKey =
          table.getResolvedSchema().getPrimaryKey().orElseThrow().getColumns();
      keyFields =
          primaryKey.stream().mapToInt(rowType.getFieldNames()::indexOf).toArray();
    }
    return new Planned(
        rowType,
        translated.planned(),
        valueFormat,
        keyFormat,
        keyFields,
        valueFields,
        translated.planned().upsert,
        null);
  }

  private static boolean supportsJsonType(LogicalType type) {
    switch (type.getTypeRoot()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case DECIMAL:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return true;
      case ROW:
      case ARRAY:
        return type.getChildren().stream().allMatch(KafkaSinkMatcher::supportsJsonType);
      default:
        return false;
    }
  }

  private static Map<String, String> options(StreamPhysicalSink sink) {
    try {
      ContextResolvedTable context = sink.contextResolvedTable();
      if (context == null) {
        return null;
      }
      ResolvedCatalogBaseTable<?> resolved = context.getResolvedTable();
      return resolved instanceof ResolvedCatalogTable
          ? ((ResolvedCatalogTable) resolved).getOptions()
          : null;
    } catch (RuntimeException ignored) {
      return null;
    }
  }

  static RelNode substitute(StreamPhysicalSink sink, PlanContext ctx) {
    KafkaSinkMatcher.Planned planned = KafkaSinkMatcher.plan(sink);
    if (planned.fallbackReason != null) {
      ctx.decline("kafka sink: " + planned.fallbackReason);
      return null;
    }
    if (!planned.upsert
        && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInputs().get(0))) {
      ctx.decline("kafka sink: the input is a changelog, not an insert-only stream");
      return null;
    }
    return new StreamPhysicalNativeKafkaSink(
        sink.getCluster(),
        sink.getTraitSet(),
        sink.getInputs().get(0),
        sink.getRowType(),
        planned);
  }
}
