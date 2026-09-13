package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.paimon.table.FileStoreTable;

/** Streaming Paimon scan with native Parquet reads and admitted native snapshot merging. */
public final class StreamPhysicalNativePaimonSource extends AbstractRelNode
    implements StreamPhysicalRel, ColumnarOutput {
  private final RelDataType output;
  private final FileStoreTable table;
  private final ScanWatermarkSpec watermark;
  private final long reuseBarrier = NativeRelDigests.nextId();

  StreamPhysicalNativePaimonSource(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelDataType output,
      FileStoreTable table,
      ScanWatermarkSpec watermark) {
    super(cluster, traits);
    this.output = output;
    this.table = table;
    this.watermark = watermark;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  protected RelDataType deriveRowType() {
    return output;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativePaimonSource(getCluster(), traits, output, table, watermark);
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return NativeRelDigests.withBarrier(
        super.explainTerms(writer)
            .item("table", table.name())
            .item("snapshotMerge", "native deduplicate / Java fallback")
            .item("fileDecode", "native Parquet"),
        reuseBarrier);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativePaimonSourceExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(output),
        getRelDetailedDescription(),
        table,
        watermark);
  }
}
