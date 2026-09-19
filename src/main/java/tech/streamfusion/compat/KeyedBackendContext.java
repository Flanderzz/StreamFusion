package tech.streamfusion.compat;

import java.util.Collection;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.util.function.FunctionWithException;

/** Host construction parameters plus a factory that preserves all remaining delegate arguments. */
public record KeyedBackendContext<K>(
    Environment environment,
    JobID jobId,
    String operatorIdentifier,
    TypeSerializer<K> keySerializer,
    int numberOfKeyGroups,
    KeyGroupRange keyGroupRange,
    Collection<KeyedStateHandle> stateHandles,
    double managedMemoryFraction,
    FunctionWithException<
            Collection<KeyedStateHandle>, CheckpointableKeyedStateBackend<K>, Exception>
        delegateFactory,
    FunctionWithException<
            Collection<KeyedStateHandle>, CheckpointableKeyedStateBackend<K>, Exception>
        canonicalProjectionFactory) {}
