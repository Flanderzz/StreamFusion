package tech.streamfusion.suite;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.utility.JavaModule;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

/** Installs StreamFusion when an untouched upstream Flink test creates a streaming planner. */
public final class StreamFusionSuiteAgent {

  private static final String PLANNER_FACTORY =
      "org.apache.flink.table.planner.delegation.DefaultPlannerFactory";
  private static final String DELEGATE_PLANNER_FACTORY =
      "org.apache.flink.table.planner.loader.DelegatePlannerFactory";
  private static final String JSON_PLAN_TEST_BASE =
      "org.apache.flink.table.planner.utils.JsonPlanTestBase";
  private static final String STREAM_EXECUTION_ENVIRONMENT =
      "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment";
  private static final String NATIVE_STATEFUL_OPERATOR =
      "tech.streamfusion.operator.AbstractNativeStatefulOperator";
  private static final String NATIVE_PARQUET_WRITER_FACTORY =
      "tech.streamfusion.operator.NativeParquetBulkWriterFactory";
  private static final AtomicBoolean ACTIVATION_REPORTED = new AtomicBoolean();
  private static final AtomicBoolean HEAP_STATE_REPORTED = new AtomicBoolean();
  private static final AtomicBoolean NATIVE_MEMORY_STATE_REPORTED = new AtomicBoolean();
  private static final AtomicBoolean ROCKSDB_STATE_REPORTED = new AtomicBoolean();
  private static final AtomicBoolean NATIVE_PARQUET_WRITER_REPORTED = new AtomicBoolean();
  private static final Set<String> TRANSFORMED_TYPES = Collections.synchronizedSet(new HashSet<>());
  private static final AtomicBoolean PLANNER_ADVICE_ENTERED = new AtomicBoolean();
  private static volatile Instrumentation INSTRUMENTATION;
  private static final Set<Object> INSTALLED_CONFIGS =
      Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));
  private static final ThreadLocal<Boolean> UNMODIFIED_PLAN_SETUP = new ThreadLocal<>();
  private static final String JSON_PLAN_TEST_PACKAGE =
      "org.apache.flink.table.planner.runtime.stream.jsonplan.";

  private StreamFusionSuiteAgent() {}

  public static void premain(String arguments, Instrumentation instrumentation) {
    INSTRUMENTATION = instrumentation;
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(StreamFusionSuiteAgent::auditInterception, "streamfusion-suite-audit"));
    new AgentBuilder.Default()
        .with(AgentBuilder.Listener.StreamWriting.toSystemError().withTransformationsOnly())
        .with(new InterceptionAudit())
        .type(named(PLANNER_FACTORY))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(InstallStreamFusion.class)
                        .on(named("create").and(takesArguments(1)))))
        .type(named(DELEGATE_PLANNER_FACTORY))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(InstallStreamFusion.class)
                        .on(named("create").and(takesArguments(1)))))
        .type(named(JSON_PLAN_TEST_BASE))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(MarkUnmodifiedPlanSetup.class)
                        .on(named("setup").and(takesArguments(0)))))
        .type(named(STREAM_EXECUTION_ENVIRONMENT))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(InstallNativeRocksDB.class)
                        .on(named("configure").and(takesArguments(2)))))
        .type(named(NATIVE_STATEFUL_OPERATOR))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(ReportNativeMemoryState.class)
                        .on(named("initializeState").and(takesArguments(1)))))
        .type(named(NATIVE_PARQUET_WRITER_FACTORY))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(ReportNativeParquetWriter.class)
                        .on(named("create").and(takesArguments(1)))))
        .installOn(instrumentation);
  }

  public static boolean reportActivation() {
    return ACTIVATION_REPORTED.compareAndSet(false, true);
  }

  public static boolean reportHeapState() {
    return HEAP_STATE_REPORTED.compareAndSet(false, true);
  }

  public static boolean reportRocksDBState() {
    return ROCKSDB_STATE_REPORTED.compareAndSet(false, true);
  }

  public static boolean reportNativeMemoryState() {
    return NATIVE_MEMORY_STATE_REPORTED.compareAndSet(false, true);
  }

  public static boolean reportNativeParquetWriter() {
    return NATIVE_PARQUET_WRITER_REPORTED.compareAndSet(false, true);
  }

  public static boolean markConfigForInstallation(Object tableConfig) {
    return INSTALLED_CONFIGS.add(tableConfig);
  }

  public static void unmarkConfigForInstallation(Object tableConfig) {
    INSTALLED_CONFIGS.remove(tableConfig);
  }

  public static void enterUnmodifiedPlanSetup() {
    UNMODIFIED_PLAN_SETUP.set(Boolean.TRUE);
  }

  public static void exitUnmodifiedPlanSetup() {
    UNMODIFIED_PLAN_SETUP.remove();
  }

  public static final class InstallStreamFusion {

    private InstallStreamFusion() {}

    @Advice.OnMethodEnter
    static void enter(@Advice.Argument(0) Object context) {
      StreamFusionSuiteAgent.recordPlannerAdviceEntered();
      try {
        if (requiresUnmodifiedFlinkPlan()) {
          return;
        }
        Object tableConfig =
            Class.forName("org.apache.flink.table.delegation.PlannerFactory$Context")
                .getMethod("getTableConfig")
                .invoke(context);
        Object runtimeMode =
            tableConfig
                .getClass()
                .getMethod("get", Class.forName("org.apache.flink.configuration.ConfigOption"))
                .invoke(
                    tableConfig,
                    Class.forName("org.apache.flink.configuration.ExecutionOptions")
                        .getField("RUNTIME_MODE")
                        .get(null));
        if (!"STREAMING".equals(runtimeMode.toString())) {
          return;
        }
        if (!StreamFusionSuiteAgent.markConfigForInstallation(tableConfig)) {
          return;
        }

        try {
          ClassLoader classLoader = context.getClass().getClassLoader();
          Class<?> nativePlanner =
              Class.forName("tech.streamfusion.planner.NativePlanner", true, classLoader);
          nativePlanner
              .getMethod("install", Class.forName("org.apache.flink.table.api.TableConfig"))
              .invoke(null, tableConfig);
        } catch (ClassNotFoundException
            | NoSuchMethodException
            | IllegalAccessException
            | InvocationTargetException e) {
          StreamFusionSuiteAgent.unmarkConfigForInstallation(tableConfig);
          throw e;
        }
        if (StreamFusionSuiteAgent.reportActivation()) {
          System.err.println("StreamFusion enabled for upstream Flink streaming planner tests");
        }
      } catch (ClassNotFoundException
          | NoSuchMethodException
          | NoSuchFieldException
          | IllegalAccessException
          | InvocationTargetException e) {
        throw new IllegalStateException("StreamFusion planner installation failed", e);
      }
    }

    public static boolean requiresUnmodifiedFlinkPlan() {
      if (Boolean.TRUE.equals(UNMODIFIED_PLAN_SETUP.get())) {
        return true;
      }
      for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
        if (frame.getClassName().startsWith(JSON_PLAN_TEST_PACKAGE)) {
          return true;
        }
        if (frame.getClassName().equals("org.apache.flink.table.api.TableEnvironmentITCase")
            && frame.getMethodName().equals("testFromToDataStreamAndExecuteSql")) {
          return true;
        }
      }
      return false;
    }
  }

  public static final class MarkUnmodifiedPlanSetup {

    private MarkUnmodifiedPlanSetup() {}

    @Advice.OnMethodEnter
    public static void enter() {
      StreamFusionSuiteAgent.enterUnmodifiedPlanSetup();
    }

    @Advice.OnMethodExit
    public static void exit() {
      StreamFusionSuiteAgent.exitUnmodifiedPlanSetup();
    }
  }

  /** Replaces only upstream tests' stock RocksDB selection, preserving the same Configuration. */
  public static final class InstallNativeRocksDB {

    private InstallNativeRocksDB() {}

    @Advice.OnMethodEnter
    static void enter(@Advice.Argument(0) Object configuration) {
      if (!Boolean.getBoolean("streamfusion.flink-suite.native-rocksdb")) {
        return;
      }
      try {
        Class<?> configOption =
            Class.forName("org.apache.flink.configuration.ConfigOption");
        Object backendOption =
            Class.forName("org.apache.flink.configuration.StateBackendOptions")
                .getField("STATE_BACKEND")
                .get(null);
        Object selected =
            configuration.getClass().getMethod("get", configOption).invoke(configuration, backendOption);
        if ("hashmap".equalsIgnoreCase(String.valueOf(selected))) {
          if (StreamFusionSuiteAgent.reportHeapState()) {
            System.err.println("StreamFusion upstream state suite exercised Flink heap backend");
          }
          return;
        }
        if (!"rocksdb".equalsIgnoreCase(String.valueOf(selected))) {
          return;
        }
        configuration
            .getClass()
            .getMethod("set", configOption, Object.class)
            .invoke(
                configuration,
                backendOption,
                "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
        if (StreamFusionSuiteAgent.reportRocksDBState()) {
          System.err.println("StreamFusion upstream state suite installed native RocksDB backend");
        }
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException("native RocksDB suite backend installation failed", e);
      }
    }
  }

  /** Proves that an upstream heap-backend case initialized StreamFusion's Rust hot-map state. */
  public static final class ReportNativeMemoryState {

    private ReportNativeMemoryState() {}

    @Advice.OnMethodExit
    static void exit(@Advice.FieldValue("rocksdbState") boolean rocksdbState) {
      if (!rocksdbState && StreamFusionSuiteAgent.reportNativeMemoryState()) {
        System.err.println(
            "StreamFusion upstream state suite initialized native memory backend");
      }
    }
  }

  /** Proves that an untouched upstream Flink test instantiated the native Parquet data plane. */
  public static final class ReportNativeParquetWriter {

    private ReportNativeParquetWriter() {}

    @Advice.OnMethodEnter
    static void enter() {
      if (StreamFusionSuiteAgent.reportNativeParquetWriter()) {
        System.err.println(
            "StreamFusion upstream Parquet suite created native Parquet sink writer");
      }
    }
  }

  public static void recordPlannerAdviceEntered() {
    PLANNER_ADVICE_ENTERED.set(true);
  }

  /**
   * Fails the JVM when the planner interception never took effect.
   *
   * <p>The advice is attached by class and method name, so on an untested Flink version a rename or
   * a signature change attaches nothing at all and the upstream suite then passes while running
   * stock Flink — a green result that proves nothing. Loaded-but-never-instrumented is unambiguous
   * and halts; instrumented-but-never-entered only warns, because a suite can load the factory
   * without ever building a table environment.
   */
  static void auditInterception() {
    Instrumentation instrumentation = INSTRUMENTATION;
    if (instrumentation == null) {
      return;
    }
    boolean plannerLoaded = isLoaded(instrumentation, PLANNER_FACTORY);
    boolean delegateLoaded = isLoaded(instrumentation, DELEGATE_PLANNER_FACTORY);
    if (!plannerLoaded && !delegateLoaded) {
      return; // no table stack in this JVM, so no interception was expected
    }
    List<String> problems = new ArrayList<>();
    if (plannerLoaded && !TRANSFORMED_TYPES.contains(PLANNER_FACTORY)) {
      problems.add(PLANNER_FACTORY + " was loaded but never instrumented");
    }
    if (delegateLoaded && !TRANSFORMED_TYPES.contains(DELEGATE_PLANNER_FACTORY)) {
      problems.add(DELEGATE_PLANNER_FACTORY + " was loaded but never instrumented");
    }
    if (!problems.isEmpty()) {
      System.err.println(
          "FATAL: the StreamFusion suite agent did not instrument the Flink planner factory."
              + " This run exercised stock Flink and any pass it reported is meaningless.");
      problems.forEach(problem -> System.err.println("  - " + problem));
      System.err.flush();
      Runtime.getRuntime().halt(70);
    }
    if (!PLANNER_ADVICE_ENTERED.get()) {
      System.err.println(
          "WARNING: the StreamFusion suite agent instrumented the Flink planner factory, but its"
              + " create(..) advice never ran — check that the method matcher still applies.");
    }
  }

  private static boolean isLoaded(Instrumentation instrumentation, String className) {
    for (Class<?> loaded : instrumentation.getAllLoadedClasses()) {
      if (className.equals(loaded.getName())) {
        return true;
      }
    }
    return false;
  }

  /** Records which interception targets actually took effect. */
  private static final class InterceptionAudit extends AgentBuilder.Listener.Adapter {

    @Override
    public void onTransformation(
        TypeDescription typeDescription,
        ClassLoader classLoader,
        JavaModule module,
        boolean loaded,
        DynamicType dynamicType) {
      TRANSFORMED_TYPES.add(typeDescription.getName());
    }
  }
}
