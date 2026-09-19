package tech.streamfusion.compat;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.*;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;

/** Adapts the host backend factory signature without owning native state or snapshot bytes. */
public abstract class FlinkStateBackendCompat implements StateBackend {
  protected final StateBackend delegate;

  public static String unsupportedNativeStateReason(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment environment,
      ReadableConfig tableConfig) {
    return null;
  }

  protected FlinkStateBackendCompat(ReadableConfig config, ClassLoader classLoader) {
    delegate = new EmbeddedRocksDBStateBackend().configure(config, classLoader);
  }

  protected abstract <K> CheckpointableKeyedStateBackend<K> createNativeKeyedBackend(
      KeyedBackendContext<K> context) throws Exception;

  @Override
  public boolean useManagedMemory() {
    return delegate.useManagedMemory();
  }

  @Override
  public final <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
      KeyedStateBackendParameters<K> parameters) throws Exception {
    return createNativeKeyedBackend(
        new KeyedBackendContext<>(
            parameters.getEnv(),
            parameters.getJobID(),
            parameters.getOperatorIdentifier(),
            parameters.getKeySerializer(),
            parameters.getNumberOfKeyGroups(),
            parameters.getKeyGroupRange(),
            parameters.getStateHandles(),
            parameters.getManagedMemoryFraction(),
            handles ->
                delegate.createKeyedStateBackend(
                    new KeyedStateBackendParametersImpl<>(parameters).setStateHandles(handles)),
            null));
  }

  @Override
  public final OperatorStateBackend createOperatorStateBackend(
      OperatorStateBackendParameters parameters) throws Exception {
    return delegate.createOperatorStateBackend(parameters);
  }
}
