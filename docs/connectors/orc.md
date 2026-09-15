# ORC

**Status:** experimental, partial. The optional `streamfusion-orc` module supplies columnar ORC
writing for streaming filesystem and Paimon sinks, and native decoding for Paimon streaming sources.
Batch jobs retain the stock connector.

## Architecture

Flink/Paimon keep file discovery, filesystem plugins, credentials, partition and bucket routing,
commits, checkpoints, compaction and statistics extraction. Production reads use released
`orc-rust` 0.9.0. Its Arrow 59 output crosses the standard C Data ABI into our Arrow 58 operators
without copying buffers. Projection order and logical types are restored, and CHAR values are
trimmed recursively in arrays, maps and rows to match Java.

Writing uses each host's released Java vectorized writer: Flink's ORC 1.5.6 or Paimon 2.0.0's
shaded ORC 1.9.8. Paimon relocates ORC and Hive classes into its own namespace, so both versions
can coexist. Two small host factories supply the writer, physical output and Hive vectors; a
shared converter accesses their public vector APIs and copies columns directly from Arrow.
Primitive loops use plain Java arrays, strings/binary share a reusable heap payload buffer per
column, and nested children are converted as whole spans. No intermediate rows, strings or
BigDecimals are constructed. See [Arrow-to-ORC vectors](../optimizations/arrow-orc-vectors.md).

The shared file codec owns an encoder object. Java ORC consumes an existing Arrow root directly;
Parquet exports it through C Data to its Rust encoder. C Data inputs to ORC are imported into a
reused Java Arrow root and released after each synchronous write. Paimon field IDs, projected
partition columns, batch slices and selected rows are preserved. The file footer records
`streamfusion.arrow.writer` after an Arrow batch is successfully written.

Java owns the seekable input and recoverable output streams. Finishing closes ORC's footer and
codec resources but leaves the host stream open. Abandoning a writer discards finalization bytes
while releasing ORC resources. Java ORC buffers and conversion arrays use Java heap memory;
Paimon's file-size estimate includes ORC's buffered estimate and conversion storage. This does
not reserve those bytes from Flink managed memory. ORC, Parquet and the engine remain separate
native libraries. There is no ORC C++ source, adapter, CMake step or Arrow C++ dependency.

Filesystem source scans stay on stock Flink, as with Parquet. Paimon streaming sources use native
Arrow decoding for append files and changelog files, and the existing native merger for admitted
primary-key snapshots. Java retains split planning, row-kind handling, projection and restore
offsets. ORC footers are inspected before a split emits data. LZO files, timestamp files written
in an unverified timezone, encrypted files, and files without enough payload or collection-count
statistics to estimate decoder memory retain Java reading.

Snapshot admission accounts for retained compressed streams, decompression scratch and
conservative whole-stripe decoded column/dictionary bounds, including null collection elements.
Declared compression block sizes are upper bounds; tiny streams are bounded by their decoded
column sizes plus encoding overhead. This is not a hard Flink managed-memory reservation.

## Configuration and types

Use normal `'format' = 'orc'` filesystem settings or Paimon's `'file.format' = 'orc'`.
Filesystem settings preserve DDL-over-Hadoop precedence, including Hive aliases. Paimon keeps its
own option resolution; explicit `orc.compress` overrides per-file compression. Shared filesystem
rolling, partition commit, naming and recovery follow the [Parquet sink](parquet.md#sink).
`streamfusion.operator.orcSink.enabled=false` disables filesystem sink substitution; Paimon's
existing source and sink switches control its ORC paths.

| Writer option | Columnar support |
|---|---|
| `orc.compress` | NONE, ZLIB, SNAPPY, LZ4; also ZSTD for Paimon's newer Java reader |
| `orc.stripe.size`, `orc.compress.size` | Stripe and compression block sizes |
| `orc.row.index.stride` | Index stride; `0` disables indexes |
| `orc.dictionary.key.threshold` | Dictionary selection threshold |
| `orc.compression.strategy` | SPEED or COMPRESSION |
| `orc.write.format` | 0.11 or 0.12 |
| `orc.bloom.filter.columns`, `orc.bloom.filter.fpp` | Host-validated column names and probability |
| Paimon `file.compression`, `file.block-size`, `file.compression.zstd-level` | Normal Paimon ORC mappings; Java ZSTD levels, independent of strategy |
| Paimon `orc.timestamp-ltz.legacy.type` | Both values in UTC; default remains Paimon's `true` |

Unsupported codecs (including LZO), unknown options and unverified non-default settings retain the
stock writer. Flink's ORC 1.5.6 reader cannot read ZSTD, so filesystem ZSTD remains stock.
Untranslated options are admitted only at the host default. The reader uses Java's configuration
whitelist; query filters remain residual Flink filters.

Both released hosts ignore `orc.create.index`; use `orc.row.index.stride = 0` to disable indexes.
The previous C++ restriction coupling ZSTD strategy with levels 1/3 no longer applies to Paimon.

Scalar booleans, integers, floating point, decimals through precision 38, strings, binary, dates
and timestamps, plus recursive arrays, maps and rows, use the host's ORC type converter.
CHAR/VARCHAR lengths and Paimon field IDs are preserved. Null parent structs and null lists with
retained child spans are supported. Paimon's type and snapshot-key gates are shared with Parquet,
including the precision-6 timestamp limit and Java fallback for floating-point snapshot keys.
Filesystem ORC retains the released Flink converter's narrower type support.

Arrow timestamp paths require a UTC JVM timezone (`-Duser.timezone=UTC`); other JVM zones retain
the stock path for schemas containing timestamps. Historical NTZ files with non-UTC stripe writer
timezones also retain Java. The released ORC decoder reads timestamps through Decimal128 nanos
before splitting them into milliseconds and a fractional remainder; years 0001–9999 retain their
precision. The writer copies components into the host timestamp vectors and mirrors
java.sql.Timestamp's historical calendar conversion for local timestamps before October 1582.
True instant columns keep their epoch-millisecond value. ORC's historical last-negative-second encoding and statistics follow Java.
That alias can make decoded timestamp keys unsorted. Native snapshot merging therefore retains
Java when a fractional timestamp is a non-leading stored key, or when a leading timestamp key's
file bounds intersect the final second before the Unix epoch. Decoding/tailing preserves Java's
encoded values; post-epoch leading timestamp keys use the native merger.

Other filesystem fallbacks match Parquet: updating inputs, overwrite, auto-compaction, zero-column
files, unsupported abilities, and sink constraints requiring stock nullability or length enforcement.
Paimon preserves its existing table-mode/configuration gates.

## Build and verification

Install `streamfusion-core`, `streamfusion-orc`, and Flink's normal `flink-orc` JAR for filesystem
sinks. Paimon supplies its shaded Java ORC classes; install its connector, Paimon module and ORC
module, following [Paimon's factory ordering](paimon.md#deployment). The ORC Maven build invokes
Cargo for its Rust reader on macOS and Linux, like the other format modules. There is no ORC
C++ toolchain/bootstrap step. See [Deployment](../deployment.md#contributing-from-source).

Run the unchanged released Flink ORC SQL tests with `bin/flink-suite.sh orc`.
The harness requires a successful columnar ORC writer marker. Local tests compare rows, recursive
field IDs and footer statistics with stock Java, across codecs, timestamps, nested slices and
selections. Shared filesystem tests cover checkpoint visibility, partition directories, footer
failure and exactly-once restoration. Paimon tests cover snapshot merging, changelogs and spilling.

The September 14, 2026 validation passed all 46 upstream Flink ORC SQL cases and 22 selected
upstream Paimon cases covering continuous reads, snapshots, changelogs, partitioned writes and
schema changes. Paimon's unchanged continuous tests use its default Parquet format; the local
Paimon SQL tests explicitly select ORC for ORC streaming coverage. This was a targeted Paimon run,
not the entire upstream suite; the existing source-reuse limitation remains documented in the
[suite guide](../upstream-flink-suite.md).

The combined development test assembly runs ORC-tagged tests in a separate Surefire execution
using ORC 1.5.6's protobuf 2.5 dependency. Flink's Protobuf format tests use protobuf 4.32.1 in the
normal execution. This mirrors the upstream format suites' separate dependency classpaths; it
does not change deployment dependencies.

## Benchmarks

These local diagnostics use release builds with mimalloc, warm local files and an Apple M1 Max,
64 GiB RAM, Java 17 in UTC. They are not whole-job throughput or peak-memory claims.

### Reading into arrow-rs

`OrcReaderComparisonBenchmark` compares production orc-rust decoding with Paimon's Java reader
followed by the production row ownership copy, row-to-Arrow conversion and C Data import into
Arrow Rust 58. The native path includes metadata admission, recursive CHAR normalization and
schema conversion. Both finish at a Rust `RecordBatch`; native batches do not return to Java.
Timing includes opening, decoding, conversion, the common integer checksum and cleanup. Untimed
scans compare full-row fingerprints. `peak_batch_bytes` describes output Arrow batches, not peak
process/decoder memory; `io_bytes` counts native reader host-callback bytes.

```bash
SF_ORC_READER_COMPARISON=true \
  mvn test -Pbench,paimon,orc-reader-bench -pl :streamfusion-paimon -am \
  -Dtest=OrcReaderComparisonBenchmark -Dsurefire.failIfNoSpecifiedTests=false
```

`SF_ORC_COMPARISON_ROWS` defaults to 262144, `SF_ORC_COMPARISON_TRIALS` to five and
`SF_ORC_COMPARISON_CODECS` to `none,zstd`. Readers rotate through two warmups and measured trials.

The [production reader measurements](../benchmarks/orc-production-readers-2026-09-14.csv) on
September 14, 2026 produced these median times for 262,144 rows:

| Read shape | Codec | Java into arrow-rs | Production Rust | Speedup |
|---|---|---:|---:|---:|
| Projected append | NONE | 114.1 ms | 17.9 ms | 6.4× |
| Full append | NONE | 359.7 ms | 57.8 ms | 6.2× |
| Projected changelog | NONE | 122.0 ms | 10.4 ms | 11.7× |
| Projected append | ZSTD | 120.3 ms | 21.8 ms | 5.5× |
| Full append | ZSTD | 382.0 ms | 65.8 ms | 5.8× |
| Projected changelog | ZSTD | 125.4 ms | 12.7 ms | 9.9× |

The [historical four-reader measurements](../benchmarks/orc-readers-2026-09-14.csv) compared the
removed nanoarrow and Arrow C++ adapters with raw orc-rust and Java. They motivated the production
Rust switch. Those raw reader timings exclude the current metadata admission and recursive CHAR
normalization; the old C++ benchmark implementations are no longer built.

### Writing from arrow-rs

`OrcWriterComparisonBenchmark` compares the production Java Arrow writer with the original Java
vector adapter. Both start from the same Rust-owned Arrow 58 batches and finish with a closed
local ORC file. Fixture generation and its one-time deep copy into Rust happen outside timing;
the fixture's Java allocator is closed before either writer runs. Both timed paths include C Data
import, column conversion, encoding, output creation, footer flush and resource cleanup. Output
close does not include `fsync`.

An untimed stock Paimon `RowDataVectorizer` writes the original rows as an independent oracle.
Java ORC compares ordered fingerprints of every column, plus schema, recursive field IDs, codec,
format version, index stride and row count, before timing and on the final timed files. Separate
fixtures cover scalar limits, precision-38 decimals, Unicode, binary, null/empty nested values,
CHAR/VARCHAR and fractional pre-epoch timestamps.

Both use 64 MiB stripes, 64 KiB compression buffers, index stride 10000, dictionary threshold 0.8,
ORC 0.12 and compression strategy SPEED by default. The harness reports file sizes alongside time.

```bash
SF_ORC_WRITER_COMPARISON=true \
  mvn test -Pbench,paimon,orc-writer-bench -pl :streamfusion-paimon -am \
  -Dtest=OrcWriterComparisonBenchmark -Dsurefire.failIfNoSpecifiedTests=false
```

`SF_ORC_WRITER_ROWS` defaults to 262144, `SF_ORC_WRITER_BATCH_ROWS` to 4096,
`SF_ORC_WRITER_TRIALS` to five and `SF_ORC_WRITER_WARMUPS` to two. Writers alternate order.
`SF_ORC_WRITER_CODECS` defaults to `NONE,ZLIB,SNAPPY,LZ4,ZSTD`; `SF_ORC_WRITER_STRATEGY` accepts
`SPEED` or `COMPRESSION`. Schemas are numeric (4 columns), string/binary (4), and nested Paimon (14).
The opt-in native helpers only retain and export benchmark Arrow fixtures.
`SF_ORC_WRITER_PROFILE=true` additionally prints handoff, conversion, encoding and residual time
for the original Java adapter. Those instrumented stage diagnostics are separate from the
uninstrumented measurements below.

The [production writer measurements](../benchmarks/orc-java-writers-2026-09-14.csv) on
September 14, 2026 show 8–29% lower elapsed time than the original Java adapter across all 15
shape/codec combinations. Selected median times for 262,144 rows are below. The C++ column is
the earlier same-machine baseline, not a paired rerun of the removed writer.

| Shape | Codec | Original Java | Production Java | Historical C++ |
|---|---|---:|---:|---:|
| Numeric, 4 columns | NONE | 20.491 ms | 14.641 ms | 12.934 ms |
| Numeric, 4 columns | ZSTD | 22.602 ms | 16.572 ms | 16.333 ms |
| Strings/binary, 4 columns | NONE | 48.316 ms | 43.127 ms | 28.844 ms |
| Strings/binary, 4 columns | ZSTD | 57.125 ms | 51.511 ms | 36.004 ms |
| Nested Paimon, 14 columns | NONE | 176.953 ms | 145.731 ms | 189.077 ms |
| Nested Paimon, 14 columns | ZSTD | 204.179 ms | 175.646 ms | 207.601 ms |

Java is within 2% of historical C++ for numeric ZSTD and faster on the wide nested fixture.
String-heavy NONE/ZSTD files still take 43–50% longer than historical C++; profiling the earlier
baseline found costs in both conversion and ORC's string encoding. The optimized converter
preserves host writer settings and leaves that encoding implementation unchanged.

The [original writer measurements](../benchmarks/orc-writers-2026-09-14.csv) retain the C++ baseline.
Its LZ4 SPEED mode used acceleration 65537 and produced almost uncompressed output, so its SPEED
timing does not represent the same compression work as Java. That artifact includes a separate
LZ4 COMPRESSION comparison. Other released native dependencies, such as Java compression codecs,
are unaffected by removing the ORC C++ integration.
