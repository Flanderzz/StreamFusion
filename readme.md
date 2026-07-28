> [!NOTE]
> This project is not part of Apache Flink or Apache DataFusion.

# StreamFusion

[![CI](https://github.com/datafusion-contrib/StreamFusion/actions/workflows/ci.yml/badge.svg)](https://github.com/datafusion-contrib/StreamFusion/actions/workflows/ci.yml)

Run Apache Flink SQL faster by executing supported operators natively (Rust + Apache
Arrow/DataFusion over JNI) while Flink continues to own planning, coordination, and
everything not yet supported. Substitution is transparent and conservative: a query is
planned by Flink, the operators we can reproduce **exactly** are swapped for native ones, and
anything else falls back to Flink with identical results.

It is DataFusion Comet's idea — a native, columnar accelerator behind an unchanged SQL
front end — applied to streaming instead of batch: stateful windowing, joins, aggregations,
changelog processing, and columnar sources/sinks, not just stateless projection and filter.

## What it accelerates

A query accelerates only when it forms **one fully-columnar island**: every operator except a
rowwise source/sink runs natively, exchanging Arrow batches (the row↔Arrow transpose is paid
once at the host edges, never between native operators). A single unsupported interior operator
drags the whole query back to Flink.

Native coverage is broad — most of the streaming SQL surface:

- **Stateless:** projection/`Calc`, filter, `UNION ALL`, `GROUPING SETS`/`CUBE`/`ROLLUP`, `UNNEST`.
- **Windowed aggregates:** `TUMBLE`/`HOP`/`SESSION`/`CUMULATE` (event-time and proctime, one- and
  two-phase), and `OVER` window functions.
- **Joins:** regular (updating) equi-joins, event-time/proctime interval and window joins,
  event-time temporal-table joins, and processing-time lookup joins (sync and async).
- **Changelog:** non-windowed `GROUP BY`, streaming Top-N / `LIMIT`, deduplication, changelog
  normalization — all consuming and emitting a retract changelog.
- **Connectors:** a Parquet file source (native Arrow scan, local paths) and a Parquet sink that
  writes to any filesystem Flink supports (`s3:`/`gs:`/`abfs:`/`hdfs:`/…, `PARTITIONED BY` and
  partition commit included — native encoding drained into Flink's own recoverable streams); Kafka
  source ingest for JSON/CSV/raw/Avro/protobuf and Debezium/OGG CDC — native rdkafka consumes and
  the independently installed format artifact decodes inside the same poll, invoked through a
  versioned C ABI it hands the connector at runtime (never linked). Watermarked Kafka tables remain
  on Flink for now.
- **UDFs:** a Flink `ScalarFunction` the expression engine can't implement itself is invoked over
  Arrow columns by a native→JVM upcall (Comet's `JvmScalarUdfExpr` pattern), one JNI crossing per
  batch, so the pipeline stays native *through* the UDF and the result is byte-identical.

The exact per-operator terms, and **every** condition that causes a fallback (unsupported
operators, types, expressions, and connector options), live in
**[docs/coverage-and-fallbacks.md](docs/coverage-and-fallbacks.md)** — the single source of truth
for what does and doesn't run natively. The short version of what stays on Flink: lateral table
functions and `MATCH_RECOGNIZE`, PyFlink UDFs, the three-phase distinct aggregate, remote
(`hdfs:`/`s3:`) file paths, a handful of expression/type edges where native execution would
diverge from the JVM (opt-in behind `allowIncompatible`), and connector options we can't yet
reproduce bit-identically (Maxwell/Canal CDC, some protobuf field types).

**Determinism.** Results are byte-identical to stock Flink for everything admitted. The one caveat
is late-data dropping on out-of-order event-time streams, where Flink is itself non-deterministic
(periodic watermarks); we match Flink's deterministic path, which governs in-order data and every
benchmark. Details in [divergences/09](divergences/09-per-batch-watermark-assignment.md).

## Inspiration

StreamFusion is built by porting established engines rather than reinventing operators:

- **[DataFusion Comet](https://github.com/apache/datafusion-comet)** — the model for the whole
  project (native columnar accelerator behind an unchanged SQL planner) and the reference for the
  JNI / Arrow C Data Interface bridge, off-heap memory accounting, the config surface, and
  fallback-reason reporting.
- **[Arroyo](https://github.com/ArroyoSystems/arroyo)** — the streaming-operator implementations
  we port (it already runs on DataFusion); the reference for join/window/changelog logic.
- **[Apache DataFusion](https://github.com/apache/datafusion)** — the native execution and
  expression engine underneath (hash joins, aggregates, Arrow kernels).
- **[RisingWave](https://github.com/risingwavelabs/risingwave)** — the reference for changelog
  semantics and memcomparable arrow-row state encoding.
- **[Apache Flink](https://github.com/apache/flink)** — the **parity target**: every operator is a
  faithful port of Flink's own, verified for identical output by a parity harness.

Divergences from these references are recorded in [`divergences/`](divergences/).

## Nexmark benchmarks

The headline benchmark is an end-to-end, exactly-once Kafka pipeline—not a blackhole sink. Stock
Flink and StreamFusion run at **parallelism 4**, read the same 2M-event Kafka JSON corpus from a
four-partition topic (one split per source subtask), and publish each query result to a fresh
Kafka topic with a one-second checkpoint interval. Append-only queries use `kafka`; updating
queries use `upsert-kafka` with the result's actual primary key. Each timed run includes source
consumption, query execution, the keyed shuffle, serialization, Kafka writes, checkpoints, and
the bounded job's final transaction commit. Between co-located subtasks StreamFusion's shuffle
moves Arrow batches by ownership transfer (zero serialization; stock Flink always serializes
across a shuffle, even in one JVM); a multi-TaskManager deployment's cross-process edges pay
Arrow IPC instead, measured at ~11% on the shuffle-heaviest mini-batch-off cells and nothing
elsewhere ([docs/optimizations.md](docs/optimizations.md)).

On StreamFusion, Kafka poll/decode, every supported operator, sink key/value/tombstone
serialization, and record production all stay native: librdkafka produces each query result inside
the checkpoint epoch's Kafka transaction, and Flink's stock Java committer commits it after the
checkpoint completes, preserving the host connector's exactly-once recovery exactly. The native
plan — including the native-producer sink shape — is asserted for every cell. q6 is omitted because
Flink SQL itself cannot run it ([analysis](.claude/wontdos/39-nexmark-q6-exclusion.md)).

These are the 2026-07-27 Apple M1 Max release+`mimalloc` results at parallelism 4, best of two
after one warmup, across all four backend/mode combinations. The memory columns compare Flink's
default heap state against StreamFusion's memory state; the disk columns compare the production
persistent backends — stock Flink on RocksDB against StreamFusion on its Paimon state backend.
Mini-batching ("on") uses the same production-style configuration on both engines
(`allow-latency=2s`, `size=50000`). Each cell is StreamFusion throughput divided by Flink
throughput within the same backend and mode.

| Query | Memory, off | Memory, on | Disk, off | Disk, on |
|---|---:|---:|---:|---:|
| q0 | **1.88×** | 0.92× | **1.78×** | **1.63×** |
| q1 | **1.26×** | **1.13×** | **1.69×** | **1.72×** |
| q2 | **1.60×** | **1.01×** | **1.48×** | **1.53×** |
| q3 | **1.06׆** | **1.06׆** | 0.86× | 0.94× |
| q4 | 0.96× | **1.38×** | **1.64×** | **2.97×** |
| q5 | **1.34×** | **1.05×** | **1.70×** | **1.56×** |
| q7 | **1.60×** | **1.59×** | **1.75×** | **2.46×** |
| q8 | **1.28×** | **1.15×** | **1.58×** | **1.48×** |
| q9 | **1.67×** | **1.86×** | **1.56×** | **1.71×** |
| q10 | **1.67×** | **1.36×** | **1.31×** | **1.33×** |
| q11 | **2.66×** | **2.68×** | **8.15×** | **8.66×** |
| q12 | **1.71×** | **1.70×** | **2.15×** | **1.85×** |
| q13 | **1.65×** | **1.60×** | **1.37×** | **1.45×** |
| q14 | **1.89×** | **1.66×** | **1.92×** | **1.55×** |
| q15 | **3.05×** | **1.75×** | **6.52×** | **2.04×** |
| q16 | **1.61×** | **1.76×** | **4.31×** | **2.61×** |
| q17 | **1.89×** | **1.76×** | **2.56×** | **1.63×** |
| q18 | **1.62×** | **1.96×** | **1.31×** | **2.66×** |
| q19 | 0.68× | **3.94×** | **1.71×** | **2.93×** |
| q20 | **3.65×** | **1.42×** | 0.93× | **1.31×** |
| q21 | **1.53×** | **1.42×** | **1.06×** | **1.08×** |
| q22 | **1.65×** | **1.26×** | **1.51×** | **1.69×** |
| q23 | **1.82×** | **1.89×** | **1.72×** | **1.46׆** |
| **geomean** | **1.63×** | **1.53×** | **1.83×** | **1.86×** |

† marks cells re-measured in a focused repeat directly after the suite (the suite cells caught
load-skewed measurements); the as-measured tables, raw timings, and method live in
[docs/benchmarks.md](docs/benchmarks.md).

Parallelism 4 is a tougher, more honest baseline than the earlier parallelism-1 tables: the keyed
shuffle is real work on both engines, and Flink's heap pipeline scales well with subtasks. The
remaining sub-parity cells are the shuffle-heavy changelog shapes with mini-batching off (q4 at
parity and q19 below it on memory state; q3 hovers at parity everywhere) — a measured
batch-collapse effect and the next scaling work (see the analysis in
[docs/benchmarks.md](docs/benchmarks.md)).
The persistent-backend columns hold up best: RocksDB pays its per-record
costs in every subtask, and the disk geomeans stay within a few points of their parallelism-1
values. The multi-source/blackhole ladder, raw timings, focused repeats, reproduction commands,
and profiling controls remain in [docs/benchmarks.md](docs/benchmarks.md).

The disk columns' key enabler is **deletion-vector mode**: stock Java Paimon maintains the
state tables' deletion vectors synchronously at each barrier, so every committed read is a raw
parquet scan with exact predicate pushdown — no merge reads, no resident index. The disk
comparison's largest wins are the stateful shapes RocksDB pays per-record for (up to 8.7× on
session windows).

_Apple M1 Max; numbers are comparable only within a machine._

## Running and configuration

### Install

#### Kubernetes or Docker

Build the universal release artifacts, then build and publish a job-neutral Flink base image:

```sh
bin/build-release.sh
bin/build-flink-image.sh --tag registry.example/streamfusion-flink:dev --push
```

Use that image as `spec.image` in a Flink Kubernetes Operator `FlinkDeployment`, or as
`kubernetes.container.image.ref` for Flink's native Kubernetes deployment. It works for either
Session or Application mode:

- **Session:** run the JobManager, TaskManagers, and the SQL/client process from the StreamFusion
  image; submit job JARs through your normal REST, SQL Gateway, or `FlinkSessionJob` path.
- **Application:** derive a job image from the StreamFusion base image, place the job JAR in
  `/opt/flink/usrlib`, and use that image in the Application deployment. Remote job-artifact
  delivery remains supported too.

The pushed tag is a Linux x86_64/ARM64 manifest. The runtime picks the matching native library
inside each pod automatically. StreamFusion itself is in Flink's `lib` directory; do not add it to
the job JAR.

The base image is connector- and format-neutral. Derive a small image and install Flink's connector
and format JARs, the matching StreamFusion connector JAR, and only the StreamFusion format JARs your
jobs use into `/opt/flink/lib`; use that same image for the JobManager, TaskManagers, and submission
client. For example, JSON on Kafka needs four JARs:

```Dockerfile
FROM registry.example/streamfusion-flink:dev
COPY flink-connector-kafka-5.0.0-2.2.jar /opt/flink/lib/
COPY flink-json-2.2.1.jar /opt/flink/lib/
COPY streamfusion-kafka/target/streamfusion-kafka-1.0-SNAPSHOT.jar /opt/flink/lib/
COPY streamfusion-json/target/streamfusion-json-1.0-SNAPSHOT.jar /opt/flink/lib/
```

Replace `streamfusion-json` with `streamfusion-csv`, `streamfusion-raw`, `streamfusion-avro`, or
`streamfusion-protobuf` and add Flink's like-named format JAR. `avro-confluent` uses the standalone
`streamfusion-avro-confluent-registry` JAR with Flink's `flink-avro-confluent-registry`. Use
`fluss-flink-2.2` with `streamfusion-fluss`, or `flink-parquet` with `streamfusion-parquet`, the
same way. The core image does not require any of them.

#### Bare metal

For a local Flink distribution instead:

```sh
bin/build-release.sh
sh bin/install-flink.sh "$FLINK_HOME"
```

Restart Flink after installation, then submit ordinary streaming SQL jobs as usual—no application
dependency or `NativePlanner.install(...)` call is needed.

StreamFusion currently supports **Flink 2.2.x**. The release build enables `mimalloc` by default.

For local development, `mvn compile` is Java-only and does not invoke Cargo. `mvn test` builds the
host debug native library once before executing tests. Build the portable optimized artifacts only
when needed for an image or release with `bin/build-release.sh`.

**Deployment JVM flags** — run the TaskManager JVM with Arrow's safety checks off (as Comet/Spark
do); profiling showed ~1/3 of the transpose CPU was per-accessor bounds/refcount checks:

```
-Darrow.enable_unsafe_memory_access=true -Darrow.enable_null_check_for_get=false
```

**Configuration** (JVM system properties, mirroring Comet's config surface):

- `-Dstreamfusion.native.enabled=false` — master switch; run entirely on Flink.
- `-Dstreamfusion.operator.<name>.enabled=false` — keep one operator on the host (e.g. leave a lone
  cheap `filter` on a row source, which can't earn back the transpose round-trip).
- `-Dstreamfusion.expression.<NAME>.allowIncompatible=true` — opt into the faster pure-Rust path for
  expressions that otherwise use a byte-exact JVM upcall or fall back (`UPPER`/`LOWER`/
  `REGEXP_EXTRACT`, `ROUND` on float, transcendental math). Off by default (parity-first).
- `-Dstreamfusion.memory.accounting.enabled` (default on) — native stateful operators reserve an
  operator-scope share of the slot's managed memory from Flink's `MemoryManager` and bound their
  state by it, failing with a `NativeMemoryLimitException` naming the remedy rather than an
  unattributed OOM ([divergences/16](divergences/16-upfront-managed-memory-reservation.md)).

**Seeing why a query fell back** — substitution is silent by default.
`-Dstreamfusion.logFallbackReasons=true` logs each node that stayed on Flink and why as the plan is
decided. `EXPLAIN` shows native nodes such as `NativeCalc` for an accelerated plan.

**Benchmarks** — the end-to-end suites (`ThroughputBenchmark`, `NexmarkBenchmark`,
`NexmarkKafkaBenchmark`, `NexmarkMatrixBenchmark`) run under
`SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench`;
the `-Pbench` profile is required (it loads the **release** native library — the debug build is
~10–20× slower and misleading). The Criterion micro-benchmarks run with `cd native && cargo bench`.
See [docs/benchmarks.md](docs/benchmarks.md).

## Related work

Three native Flink accelerators exist, all **closed source**:

- **Flash** (Alibaba Cloud) — a C++ native + SIMD vectorized engine with a custom state backend
  (ForStDB). Stateful, production-deployed at scale; claims 5–10× on streaming Nexmark, 3×+ on batch
  TPC-DS, and ~50% cost reduction across 100k+ compute units. Proprietary, on Alibaba Cloud.
  ([blog](https://www.alibabacloud.com/blog/flash-a-next-gen-vectorized-stream-processing-engine-compatible-with-apache-flink_602088))
- **Vera X** (Ververica, the original Flink creators) — a proprietary native vectorized engine with
  a drop-in compatibility layer and a new state store. Stateful; claims 5–10× on Nexmark SQL and
  ~52% lower resource usage. Implementation undisclosed.
  ([blog](https://www.ververica.com/blog/vera-x-introducing-the-first-native-vectorized-apache-flink-engine))
- **Iron Vector** (Irontools) — the same stack as us (Rust + Arrow + DataFusion over zero-copy JNI,
  Substrait plan serialization, transparent fallback), but **stateless only** today (projections,
  filters, expressions); windows, joins, and exactly-once are described as planned. Claims ~97%
  higher throughput on a stateless ETL pipeline.
  ([blog](https://irontools.dev/blog/introducing-iron-vector/))

Where StreamFusion differs: it is **open source**, and every substitution is gated and verified for
identical results against stock Flink by a parity harness rather than asserted. It is already native
on stateful windowing, joins, and changelog processing — the hard, closed part of the field — where
Iron Vector is stateless-only; it is earlier-stage than Flash and Vera X and doesn't match their
operator breadth or published benchmarks, but its acceleration is auditable and parity-first by
construction.

## License

Licensed under the Apache License, Version 2.0 ([LICENSE](LICENSE) or
<https://www.apache.org/licenses/LICENSE-2.0>).

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in the work by you, as defined in the Apache-2.0 license, shall be licensed
as above, without any additional terms or conditions.
