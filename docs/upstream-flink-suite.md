# Upstream Flink suite

StreamFusion can run Flink's own table planner runtime integration tests with native acceleration
installed in every streaming planner. This follows the purpose of DataFusion Comet's upstream Spark
SQL jobs while keeping Flink stricter: it uses an unmodified release tag and injects StreamFusion
only into the forked test JVM rather than copying or patching upstream tests.

Run the planner runtime suite from the repository root:

```bash
bin/flink-suite.sh
```

The same harness also runs Flink's unchanged format integration tests, the Kafka connector's
unchanged table SQL integration tests, and Paimon's unchanged append-table SQL integration tests:

```bash
bin/flink-suite.sh formats
bin/flink-suite.sh parquet
bin/flink-suite.sh orc
bin/flink-suite.sh kafka
bin/flink-suite.sh paimon
bin/flink-suite.sh all
```

`formats` covers Flink's JSON (including Debezium and Ogg CDC), CSV, Avro, and Protobuf integration
tests and compiles the Confluent Avro module (the pinned release contains no integration test in
that module). `parquet` runs Flink's unchanged `ParquetFsStreamingSinkITCase` and
`ParquetTimestampITCase`, and fails unless the suite proves that a native Parquet writer was
created. `orc` runs `OrcFsStreamingSinkITCase` and `OrcFileSystemITCase` and requires a successful
columnar ORC writer marker (the writer now uses the host's Java ORC vectors). The harness runs timestamp tests with a UTC JVM. `kafka` covers `DynamicKafkaTableITCase`, `KafkaChangelogTableITCase`,
`KafkaTableITCase`, and `UpsertKafkaTableITCase` from the pinned Kafka connector release. The Kafka
suite starts broker containers and therefore requires a working Docker daemon. `paimon` runs the
Paimon Flink connector's `AppendOnlyTableITCase`, `AppendTableITCase`, `BatchFileStoreITCase`,
`ComputedColumnAndWatermarkTableITCase`, `ContinuousFileStoreITCase`, `ReadWriteTableITCase`,
`PrimaryKeyFileStoreTableITCase`, `CompositePkAndMultiPartitionedTableITCase`,
`FullCompactionFileStoreITCase`, `FlinkJobRecoveryITCase`, `RescaleBucketITCase`,
`ScanBucketITCase`, `KeyOnlyDeletesITCase`, `FirstRowITCase`, and `CoordinatorCommitITCase` from the
pinned Paimon release,
built against the suite's Flink version, and fails unless it proves both that a streaming insert
wrote an append-table data file from a native Arrow bundle and that one wrote a primary-key
level-0 file natively, and that a native snapshot merger emitted an Arrow batch. The markers are
emitted only after the corresponding write or read returns successfully.
The agent selects the same complete-plan streaming hook as the deployed planner factory.
`ContinuousFileStoreITCase.testSourceReuseWithScanPushDown` passes unchanged: compatible projected
scans share one native source, while filtered and limited scans stay separate.
Streaming inserts covered by the [Paimon connector whitelist](connectors/paimon.md)
can take the native sink, including coordinated writers, coordinator commits, dynamic
partition routing, and automatic append-buffer spilling. The regular StreamFusion SQL parity
suite forces Arrow spilling and checkpoint failure/recovery; the unchanged upstream streaming
tests exercise the surrounding writer lifecycle. Batch inserts,
unsupported primary-key options, and compaction rewrites use stock Paimon. `CoordinatorCommitITCase`
checks removal of the global committer, coordinator metrics, committed rows, and snapshot watermark
parity for active and idle inputs. Because
Surefire appends StreamFusion's classpath in no fixed order, the agent also resolves Paimon's
`parquet` and `orc` format identifiers to the StreamFusion factories whenever the module is present, standing in
for the `01-streamfusion-paimon.jar` ordering a deployment relies on. Paimon's module declares the
planner test-jar before the planner itself, which would place stock Calcite ahead of Flink's patched
validator classes (breaking `CALL` procedures and time travel in stock tests), so the runner drops
the resolved calcite-core from that module's test classpath and appends it after the planner
instead. `all` runs formats, Parquet, ORC,
the planner runtime suite, Paimon, and Kafka in that order.

The runner clones Flink `release-2.2.1`, Kafka connector `v5.0.0`, and Paimon `2.0.0` (its
`release-2.0.0-rc10` tag) under `.flink-suite`, verifies that each checkout is clean, builds and
installs StreamFusion and its supported format/connector modules, and builds the required upstream
reactors with tests skipped. A test-only
Java agent then installs StreamFusion whenever an upstream test creates a streaming planner, and
loads the native library at that moment the way a TaskManager loads it once at startup, so no
upstream job pays the first-load latency inside its first native task; batch planners remain stock
Flink. The default run executes the planner module's unchanged `*ITCase`
runtime integration suite serially in one fork, then summarizes Surefire failures. Serial execution
keeps concurrently created MiniClusters from exhausting a developer machine or CI runner.

Flink's published planner artifact relocates its internal Calcite classes, while its source tests use
the unshaded classes. The runner therefore keeps an isolated Maven repository and compiles
an isolated copy of the StreamFusion source tree against the checkout's untouched parser, Calcite
bridge, and planner output. Those unshaded artifacts are installed with the dependencies their
shaded form bundles declared as ordinary dependencies, the same way Flink's IntelliJ profile exposes
them, so every upstream test module resolves Flink's patched Calcite classes through its own planner
dependency, ahead of stock Calcite. Surefire does not preserve the order of the StreamFusion classpath
it appends, so nothing may depend on that order for class resolution. Suite-only artifacts remain
under `.flink-suite`; production build outputs and the developer's normal Maven repository are not
replaced. Test JVMs load the engine and optional native libraries from the isolated source build's
`native/target/debug` directory through `java.library.path`, as required by development mode.

Flink's plan unit tests assert stock physical operator names, so an accelerator necessarily changes
their golden output. Run `bin/flink-suite.sh diagnostic` to include those tests when inspecting plan
coverage; their `Calc` versus `NativeCalc`-style diffs are diagnostic output, not result-parity bugs.

The checkout is cached between runs. Set `FLINK_SUITE_ROOT` to put it elsewhere, or tune local test
parallelism with `FLINK_SUITE_UNIT_FORKS` and `FLINK_SUITE_IT_FORKS`. The runner uses only public
artifact repositories, independent of developer-specific Maven mirrors. `FLINK_VERSION` is pinned by
the harness and should only be changed after validating the injection point against that release.

After a successful build, skip the StreamFusion and Flink rebuild while iterating on test selection:

```bash
FLINK_SUITE_REUSE_BUILD=true FLINK_SUITE_TEST='org.apache.flink.table.planner.runtime.stream.sql.CalcITCase' bin/flink-suite.sh runtime
```

JSON compiled-plan tests and the one Table API test that asserts Flink's exact operator names still
run, but their planners intentionally remain stock Flink: an accelerator's additional exec-node types
and operator names are outside those tests' contract. All other streaming planners receive
StreamFusion. The runner also reports Flink's independently reproducible batch `CURRENT_DATE`
timezone failure as an expected upstream failure instead of attributing it to StreamFusion.

During development, select one or more Surefire test classes without changing the upstream checkout:

```bash
FLINK_SUITE_TEST='org.apache.flink.table.planner.runtime.stream.sql.CalcITCase,org.apache.flink.table.planner.runtime.stream.table.CalcITCase' bin/flink-suite.sh
```

The same `FLINK_SUITE_TEST` and `FLINK_SUITE_REUSE_BUILD=true` controls apply to `formats`,
`parquet`, `orc`, `kafka`, and `paimon`. Reuse mode requires that the selected mode has been built once normally.

The focused Paimon coordinator run includes its four paged writer-restoration cases, three
commit-coordinator cases, and a deterministic primary-key write to verify native file creation:

```bash
FLINK_SUITE_TEST='org.apache.paimon.flink.CoordinatorCommitITCase,org.apache.paimon.flink.BatchFileStoreITCase#testWriteRestoreCoordinator*,org.apache.paimon.flink.ReadWriteTableITCase#testStreamingReadWriteWithPartitionedRecordsWithPk' \
  bin/flink-suite.sh paimon
```

The focused streaming dynamic-partition run uses Paimon's unchanged skewed-input SQL test,
alongside a primary-key write to satisfy the suite's two native-write checks:

```bash
FLINK_SUITE_TEST='org.apache.paimon.flink.AppendTableITCase#testPartitionDynamicStreaming,org.apache.paimon.flink.ReadWriteTableITCase#testStreamingReadWriteWithPartitionedRecordsWithPk' \
  bin/flink-suite.sh paimon
```

The Flink checkout remains byte-for-byte unchanged. Every push to `main` and every pull request
runs all seven upstream suites in GitHub Actions: planner runtime, formats, Parquet, ORC, Kafka,
Paimon and state/recovery. The weekly schedule and manual dispatch run the same complete matrix.
Each run rebuilds StreamFusion from that revision in the isolated suite directory and uploads
its complete build/test log with the commit SHA. These checks complement the released-artifact
SQL parity tests in ordinary CI; a passing local Maven suite alone does not establish upstream
integration compatibility.

Merges to `main` require **All CI tests** and **All upstream integration tests**, enforced by
the repository's **Require all test suites** ruleset with no bypass actors, including administrators.
The first check waits for Rust, Java/SQL parity, every format/connector module, both Paimon formats,
Delta and the deployed Flink image integration job. The second waits for all seven upstream suites.
Each check runs even when a dependency fails and succeeds only when every dependency succeeds;
failed, cancelled or unexpectedly skipped jobs cannot produce a green aggregate check. Matrix
additions are included automatically; new independent test jobs must be added to the corresponding
aggregate's `needs` list. These two check names are part of the merge contract and must stay aligned
with the GitHub ruleset. The existing PR and other branch-protection rules remain in place.

Before committing operator changes, run the relevant unchanged upstream integration classes
alongside the local SQL parity and recovery tests. Record the class selection and actual result
in the commit. A selected run is not the full upstream suite, and pending CI is not a passing result.
`FLINK_SUITE_REUSE_BUILD=true` reuses the existing StreamFusion binaries as well as Flink's;
omit it after source changes so the upstream tests execute the current implementation.

The validated Flink 2.2.1 baseline is 8,619 tests: 8,570 passed, 48 skipped by Flink, zero unexpected
failures or errors, and the one independently reproduced `CURRENT_DATE` xfail described above.
The format baseline is 185 tests: 175 passed and 10 skipped by Flink. The Kafka SQL baseline is 86
tests, all passed. The Parquet sink baseline is 8 tests, all passed, including the suite's explicit
proof that Flink instantiated the native Parquet writer. The complete Paimon baseline is 265
tests, all passed with native source sharing, including native append-write, primary-key-write,
and snapshot-merge markers. The complete-plan hook also passed 816 targeted Flink join, Calc and
JSON function cases.
The ORC Java-writer validation on September 14, 2026 passed all 46 unchanged Flink ORC SQL tests.
A targeted upstream Paimon run passed 22 continuous-read, partition-write and schema-change cases;
the [ORC page](connectors/orc.md#build-and-verification) distinguishes that run from local tests
that explicitly exercise ORC streaming.

## Expected host failures in SQL parity audits

Released Flink 2.2.1/JDK 17 fails these expressions even without StreamFusion or an audit source
adapter. Both a one-row DataStream table containing `doc = '{"v":1}'` and a SQL VALUES table
reproduce them:

| Expression | Resolved SQL result type | Host conversion failure |
|---|---|---|
| `JSON_VALUE(doc, '$.v' RETURNING BOOLEAN NULL ON ERROR)` | BOOLEAN | `Integer` to `Boolean` |
| `JSON_VALUE(doc, '$.v' RETURNING DOUBLE NULL ON ERROR)` | DOUBLE | `Integer` to `BigDecimal` |

The failure is a `ClassCastException` during scalar result conversion, after JSON parsing and
path evaluation succeed. Flink's generated BOOLEAN conversion casts the selected object directly
to `Boolean`; DOUBLE casts it to `BigDecimal` before extracting a double. That conversion happens
outside JSON_VALUE's ON ERROR policy. The source column is correctly typed STRING; matching the
declared SQL result schema does not coerce the JSON token's Java object type.

`FlinkJsonReturningHostContractTest` is the independent reproducer and checks the inferred types
and exception causes. Separate controls with JSON `true`, `1.0`, and integer `1` for RETURNING
BOOLEAN, DOUBLE, and INTEGER respectively succeed and match native execution. Run it with
`mvn -pl streamfusion-runtime -am test -Dtest=FlinkJsonReturningHostContractTest`.

Upstream tracking is [FLINK-40463](https://issues.apache.org/jira/browse/FLINK-40463) and
[Flink PR #29063](https://github.com/apache/flink/pull/29063). The proposed conversion layer
replaces exact Java object casts and brings conversion errors under ON ERROR handling;
the PR explicitly reproduces DOUBLE conversion failing on integer JSON tokens. As of
September 16, 2026 it is open. StreamFusion keeps released Flink 2.2.1 and its explicit
exception expectations; a future released dependency upgrade must revalidate that contract.

The audit must retain the original JSON tokens and classify these cases as expected host
failures, rather than missing fixtures, native fallback, or successful result parity. Do not
rewrite integer `1` to decimal `1.0` just to obtain a successful baseline. Native scalar-conversion failures preserve the host ClassCastException and its source/target
Java types through a typed error channel, including in filters and across multiple batches.

## Comparing failed SQL executions

`NativeFailureParity` runs stock Flink and the native-enabled query independently from fresh
fixture factories. The host outcome is captured before the native attempt; no assertion or
expected host error can skip that second attempt. Each outcome retains the exception chain,
collected rows with RowKind, planner substitution/fallback status and fallback reasons.

The helper records setup, planning, submission and collection boundaries separately. During
submission or collection, the originating exception stack can identify operator initialization
(`open`/`initializeState`) or row evaluation (`eval`, accumulation, processing, or end-of-input).
Without that evidence it preserves the observed boundary, rather than claiming to know where a
remote failure originated. A source failure delivered by the collect iterator is one such case.
The route describes the plan: a native operator whose `open` fails has not evaluated any rows.

Failure assertions require both executions to fail, matching root-cause classes and a meaningful
message fragment, the expected phase, and an explicit native or fallback route. Success controls
require both to succeed and compare collected results. Wrapper exception text and stack traces
need not match. Partial output is retained for inspection, but asynchronous failed jobs do not
promise identical delivered prefixes; tests assert prefixes only where the fixture defines them.
The single malformed-decimal input, for example, yields no collected rows on either engine.

`FlinkFailureParitySqlHarnessTest` covers:

- SINGLE_VALUE cardinality errors through explicit planner fallback, and malformed runtime DECIMAL
  casts with actual native substitution and identical NumberFormatException messages.
- CASE short-circuiting, JSON NULL/DEFAULT ON ERROR, and native TRY_CAST-to-DECIMAL conversion failures.
- Planning rejection, UDF initialization failure and a source failure observed during collection.
- Deliberate success/failure mismatches in either direction, which must fail the parity assertion.
- JSON RETURNING scalar-conversion errors: both engines throw ClassCastException with identical
  source/target type diagnostics on the tested JDK 17 baseline. Cases cover Integer, Long,
  BigInteger, BigDecimal, Boolean and String input objects, native projections and filters,
  and a failure after multiple batches. The native kernel carries a structured error through
  DataFusion; the guarded JNI boundary raises the Java exception after Arrow buffers unwind.

This does not establish identical diagnostics for every native error. JSON ERROR policies and
native integer parsing still use their existing generic native exception wrapper.
The JSON path grammar suite also uses `NativeFailureParity` to check scalar-conversion failures
against Flink's root exception, independently of its TableRuntimeException wrappers.
Malformed STRING-to-BOOLEAN tests preserve literal NUL characters in diagnostic text;
the JNI exception message no longer escapes them as a backslash and zero.

Run the failure suite and independent host reproducer together:

```bash
mvn -pl streamfusion-runtime -am test -Dtest=FlinkFailureParitySqlHarnessTest,FlinkJsonReturningHostContractTest
```
