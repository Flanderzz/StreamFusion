package tech.streamfusion.suite;

/** Preserves legacy upstream checkpoint storage while selecting the native keyed backend. */
public final class LegacyStateBackend {
  private LegacyStateBackend() {}

  public static Object replace(Object environment, Object backend) {
    if (!Boolean.getBoolean("streamfusion.flink-suite.native-rocksdb") || backend == null) {
      return backend;
    }
    String type = backend.getClass().getName();
    if (type.equals("org.apache.flink.runtime.state.memory.MemoryStateBackend")
        || type.equals("org.apache.flink.runtime.state.filesystem.FsStateBackend")
        || type.equals("org.apache.flink.runtime.state.hashmap.HashMapStateBackend")) {
      if (StreamFusionSuiteAgent.reportHeapState()) {
        System.err.println("StreamFusion upstream state suite exercised Flink heap backend");
      }
      return backend;
    }
    if (!type.equals("org.apache.flink.contrib.streaming.state.RocksDBStateBackend")) {
      return backend;
    }
    try {
      Object checkpointConfig =
          environment.getClass().getMethod("getCheckpointConfig").invoke(environment);
      Object storage = backend.getClass().getMethod("getCheckpointBackend").invoke(backend);
      checkpointConfig
          .getClass()
          .getMethod(
              "setCheckpointStorage",
              Class.forName("org.apache.flink.runtime.state.CheckpointStorage"))
          .invoke(checkpointConfig, storage);
      Class<?> configurationType = Class.forName("org.apache.flink.configuration.Configuration");
      Object configuration =
          configurationType
              .getConstructor(configurationType)
              .newInstance(
                  environment.getClass().getMethod("getConfiguration").invoke(environment));
      configurationType
          .getMethod("setBoolean", String.class, boolean.class)
          .invoke(
              configuration,
              "state.backend.incremental",
              backend.getClass().getMethod("isIncrementalCheckpointsEnabled").invoke(backend));
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      Object factory =
          Class.forName("tech.streamfusion.state.RocksDBNativeStateBackendFactory", true, loader)
              .getConstructor()
              .newInstance();
      Object replacement =
          factory
              .getClass()
              .getMethod(
                  "createFromConfig",
                  Class.forName("org.apache.flink.configuration.ReadableConfig"),
                  ClassLoader.class)
              .invoke(factory, configuration, loader);
      if (StreamFusionSuiteAgent.reportRocksDBState()) {
        System.err.println("StreamFusion upstream state suite installed native RocksDB backend");
      }
      return replacement;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("legacy native RocksDB suite backend installation failed", e);
    }
  }
}
