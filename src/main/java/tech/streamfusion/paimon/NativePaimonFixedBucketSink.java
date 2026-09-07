package tech.streamfusion.paimon;

import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.FlinkWriteSink;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.operator.BucketedArrowBatch;

/** Paimon's fixed-bucket sink with the row-fed write operator swapped for the bundle-fed one. */
public final class NativePaimonFixedBucketSink extends FlinkWriteSink<BucketedArrowBatch> {
  private static final long serialVersionUID = 1L;

  public NativePaimonFixedBucketSink(FileStoreTable table) {
    super(table, null);
  }

  @Override
  protected OneInputStreamOperatorFactory<BucketedArrowBatch, Committable>
      createWriteOperatorFactory(StoreSinkWrite.Provider writeProvider, String commitUser) {
    return new NativePaimonWriteOperator.Factory(table, writeProvider, commitUser, false);
  }
}
