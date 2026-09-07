package tech.streamfusion.planner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.planner.plan.abilities.sink.OverwriteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.PartitionSinkStrategy;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.flink.DataCatalogTable;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.FlinkFileIOLoader;
import org.apache.paimon.flink.sink.FlinkTableSink;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import tech.streamfusion.paimon.NativePaimonParquetFormat;

/**
 * Whitelist-first admission for the columnar Paimon append-table sink. The table is resolved the
 * way Paimon's own Flink factory resolves it, so every option the stock sink would see — DDL,
 * hints, and the table-scoped dynamic options — shapes the native topology identically.
 */
final class PaimonSinkMatcher {
  private PaimonSinkMatcher() {}

  static final class Planned {
    final FileStoreTable table;
    final int[] partitionColumns;
    final int[] partitionTimestampPrecisions;
    final int[] bucketColumns;
    final int[] bucketTimestampPrecisions;
    final String fallbackReason;

    private Planned(
        FileStoreTable table,
        int[] partitionColumns,
        int[] partitionTimestampPrecisions,
        int[] bucketColumns,
        int[] bucketTimestampPrecisions,
        String fallbackReason) {
      this.table = table;
      this.partitionColumns = partitionColumns;
      this.partitionTimestampPrecisions = partitionTimestampPrecisions;
      this.bucketColumns = bucketColumns;
      this.bucketTimestampPrecisions = bucketTimestampPrecisions;
      this.fallbackReason = fallbackReason;
    }

    static Planned fallback(String reason) {
      return new Planned(null, null, null, null, null, reason);
    }
  }

  static boolean appliesTo(StreamPhysicalSink sink) {
    return sink.tableSink() instanceof FlinkTableSink;
  }

  static Planned plan(StreamPhysicalSink sink) {
    for (SinkAbilitySpec ability : sink.abilitySpecs()) {
      if (ability instanceof OverwriteSpec) {
        return Planned.fallback("INSERT OVERWRITE is not supported");
      }
    }
    if (!ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInput())) {
      return Planned.fallback("an append table takes an insert-only input");
    }
    FileStoreTable table = resolveTable(sink);
    if (table == null) {
      return Planned.fallback("the sink is not a Paimon data table");
    }
    CoreOptions coreOptions = table.coreOptions();
    Options options = coreOptions.toConfiguration();
    if (!table.primaryKeys().isEmpty()) {
      return Planned.fallback("primary-key tables are not supported yet");
    }
    BucketMode bucketMode = table.bucketMode();
    if (bucketMode != BucketMode.HASH_FIXED && bucketMode != BucketMode.BUCKET_UNAWARE) {
      return Planned.fallback("bucket mode " + bucketMode + " is not supported");
    }
    if (!"parquet".equalsIgnoreCase(coreOptions.fileFormatString())) {
      return Planned.fallback("file.format " + coreOptions.fileFormatString() + " is not supported");
    }
    if (!options.get(CoreOptions.FILE_FORMAT_PER_LEVEL).isEmpty()) {
      return Planned.fallback("file.format.per.level is not supported");
    }
    if (options.get(CoreOptions.WRITE_BUFFER_FOR_APPEND)) {
      return Planned.fallback("write-buffer-for-append is not supported");
    }
    if (!new FileIndexOptions(coreOptions).isEmpty()) {
      return Planned.fallback("file indexes are not supported");
    }
    if (coreOptions.rowTrackingEnabled()) {
      return Planned.fallback("row tracking is not supported");
    }
    if (coreOptions.dataEvolutionEnabled()) {
      return Planned.fallback("data evolution is not supported");
    }
    if (!CoreOptions.blobField(table.options()).isEmpty()) {
      return Planned.fallback("BLOB columns are not supported");
    }
    if (!coreOptions.clusteringColumns().isEmpty()) {
      return Planned.fallback("sink clustering is not supported");
    }
    if (coreOptions.partitionSinkStrategy() == PartitionSinkStrategy.PARTITION_DYNAMIC) {
      return Planned.fallback("partition.sink-strategy PARTITION_DYNAMIC is not supported");
    }
    if (options.get(FlinkConnectorOptions.SINK_WRITER_COORDINATOR_ENABLED)) {
      return Planned.fallback("sink.writer-coordinator.enabled is not supported");
    }
    if (options.get(FlinkConnectorOptions.SINK_COORDINATOR_COMMIT_ENABLED)) {
      return Planned.fallback("sink.coordinator-commit.enabled is not supported");
    }
    RelDataType inputType = sink.getInput().getRowType();
    List<String> fieldNames = table.rowType().getFieldNames();
    if (!inputType.getFieldNames().equals(fieldNames)) {
      return Planned.fallback("the sink input columns do not match the Paimon table schema");
    }
    String formatFallback = formatFallbackReason(table);
    if (formatFallback != null) {
      return Planned.fallback(formatFallback);
    }
    int[] partitionColumns = ordinals(fieldNames, table.partitionKeys());
    int[] bucketColumns =
        bucketMode == BucketMode.HASH_FIXED
            ? ordinals(fieldNames, table.schema().bucketKeys())
            : new int[0];
    return new Planned(
        table,
        partitionColumns,
        FlinkKeyGroupUtils.timestampPrecisions(inputType, partitionColumns),
        bucketColumns,
        FlinkKeyGroupUtils.timestampPrecisions(inputType, bucketColumns),
        null);
  }

  /**
   * Paimon picks the first {@code parquet} format factory on the classpath. Writing natively needs
   * ours to have won that discovery, so a table whose files would still be written by Paimon's own
   * format is declined loudly rather than silently left on the stock path.
   */
  static String formatFallbackReason(FileStoreTable table) {
    Options options = table.coreOptions().toConfiguration();
    FileFormat format = FileFormat.fromIdentifier("parquet", options);
    if (!(format instanceof NativePaimonParquetFormat)) {
      return "Paimon resolved the parquet format to "
          + format.getClass().getName()
          + "; streamfusion-paimon must precede paimon-flink on the classpath"
          + " (deploy it as 01-streamfusion-paimon.jar)";
    }
    return ((NativePaimonParquetFormat) format).nativeWriterFallbackReason(table.rowType());
  }

  private static FileStoreTable resolveTable(StreamPhysicalSink sink) {
    ContextResolvedTable resolved = sink.contextResolvedTable();
    ResolvedCatalogBaseTable<?> resolvedTable = resolved.getResolvedTable();
    CatalogBaseTable origin = resolvedTable.getOrigin();
    Map<String, String> options = new HashMap<>(resolvedTable.getOptions());
    options.putAll(PaimonDynamicOptions.forTable(sink, resolved.getIdentifier()));
    FileStoreTable table;
    if (origin instanceof DataCatalogTable) {
      Table paimonTable = ((DataCatalogTable) origin).table();
      if (!(paimonTable instanceof FileStoreTable)) {
        return null;
      }
      table = (FileStoreTable) paimonTable;
    } else if (options.containsKey(CoreOptions.PATH.key())) {
      table =
          FileStoreTableFactory.create(
              CatalogContext.create(Options.fromMap(options), new FlinkFileIOLoader()));
    } else {
      return null;
    }
    return table.copyWithoutTimeTravel(options);
  }

  private static int[] ordinals(List<String> fieldNames, List<String> columns) {
    return columns.stream().mapToInt(fieldNames::indexOf).toArray();
  }

  static RelNode substitute(StreamPhysicalSink sink, PlanContext context) {
    if (!NativeConfig.operatorEnabled("paimonSink")) {
      context.decline(Substitution.disabledReason("paimonSink"));
      return null;
    }
    Planned planned = plan(sink);
    if (planned.fallbackReason != null) {
      context.decline("paimon sink: " + planned.fallbackReason);
      return null;
    }
    return new StreamPhysicalNativePaimonSink(
        sink.getCluster(), sink.getTraitSet(), sink.getInput(), sink.getRowType(), planned);
  }
}
