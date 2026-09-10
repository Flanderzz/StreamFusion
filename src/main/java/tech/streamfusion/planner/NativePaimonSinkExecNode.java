package tech.streamfusion.planner;

import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.CoreOptions.PartitionSinkStrategy;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBucketRouter;
import tech.streamfusion.operator.BucketedArrowBatch;
import tech.streamfusion.operator.BucketedArrowBatchTypeInformation;
import tech.streamfusion.paimon.BucketedArrowBatchChannelComputer;
import tech.streamfusion.paimon.NativePaimonAppendSink;
import tech.streamfusion.paimon.NativePaimonFixedBucketSink;

/**
 * Builds the columnar Paimon append topology: batches are split natively per (partition, bucket),
 * shuffled with Paimon's own channel formula while still Arrow, and handed to Paimon's write and
 * commit operators as bundles. The shuffle and parallelism rules mirror Paimon's sink builder so
 * the resulting job graph differs from the stock one only in the record type crossing the edge.
 */
public final class NativePaimonSinkExecNode extends ExecNodeBase<Object>
    implements StreamExecNode<Object>, SingleTransformationTranslator<Object> {

  private final PaimonSinkMatcher.Planned planned;

  public NativePaimonSinkExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      PaimonSinkMatcher.Planned planned) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-paimon-sink_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.planned = planned;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<Object> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    FileStoreTable table = planned.table;
    boolean fixedBucket = table.bucketMode() == BucketMode.HASH_FIXED;
    int numBuckets = fixedBucket ? table.bucketSpec().getNumBuckets() : -1;
    Integer parallelism =
        Options.fromMap(table.options())
            .getOptional(FlinkConnectorOptions.SINK_PARALLELISM)
            .orElse(null);

    OneInputTransformation<ArrowBatch, BucketedArrowBatch> route =
        new OneInputTransformation<>(
            input,
            "native-paimon-bucket-route",
            SimpleOperatorFactory.of(
                new ArrowBucketRouter(
                    planned.partitionColumns,
                    planned.partitionTimestampPrecisions,
                    planned.bucketColumns,
                    planned.bucketTimestampPrecisions,
                    numBuckets)),
            BucketedArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    DataStream<BucketedArrowBatch> routed = new DataStream<>(planner.getExecEnv(), route);
    int partitionArity = table.partitionKeys().size();

    DataStreamSink<?> end;
    if (fixedBucket) {
      if (parallelism == null && numBuckets < routed.getParallelism() && partitionArity == 0) {
        parallelism = numBuckets;
      }
      DataStream<BucketedArrowBatch> partitioned =
          FlinkStreamPartitioner.partition(
              routed, BucketedArrowBatchChannelComputer.byBucket(partitionArity), parallelism);
      end = new NativePaimonFixedBucketSink(table, false).sinkFrom(partitioned);
    } else {
      DataStream<BucketedArrowBatch> shuffled = routed;
      if (partitionArity > 0
          && table.coreOptions().partitionSinkStrategy() == PartitionSinkStrategy.HASH) {
        shuffled =
            FlinkStreamPartitioner.partition(
                routed, BucketedArrowBatchChannelComputer.byPartition(partitionArity), parallelism);
      }
      end = new NativePaimonAppendSink(table, parallelism).sinkFrom(shuffled);
    }
    return (Transformation<Object>) (Transformation<?>) end.getTransformation();
  }
}
