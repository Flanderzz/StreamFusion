# ORC

**Status:** experimental, partial. The optional `streamfusion-orc` module supplies native ORC
encoding for streaming filesystem sinks and native decoding/encoding for [Paimon](paimon.md).
Batch jobs retain the stock connector.

## Architecture

Flink/Paimon keep file discovery, filesystem plugins, credentials, partition and bucket routing
rules, commits, checkpoints, compaction and statistics extraction. Apache ORC C++ reads and writes
ORC vectors; a small nanoarrow adapter converts those vectors to/from Arrow C Data. This copies
columns between ORC and Arrow representations, without materializing Java rows. Java owns the
seekable input and recoverable output streams, with bounded JNI transfers. ORC, Parquet and the
engine remain separate native libraries.

Filesystem source scans stay on stock Flink, as with Parquet. Paimon streaming sources use native
Arrow decoding for append files and changelog files, and the existing native merger for admitted
primary-key snapshots. The same Java split planning, row-kind handling, projection, restore offsets
and fallback rules apply to both formats. ORC footers are inspected before a split emits data.
LZO files, timestamp files written in an unverified timezone, and files without enough variable-width
statistics to estimate decoder memory, retain Java reading. Snapshot admission adds conservative
decoded dictionary/payload estimates to ORC's compressed-stripe/decompressor estimate; this is not
a hard Flink managed-memory reservation.

## Configuration and types

Use the normal `'format' = 'orc'` filesystem option or Paimon's `'file.format' = 'orc'`.
There is no parallel StreamFusion ORC configuration namespace. Filesystem settings preserve
DDL-over-Hadoop precedence, including Hive aliases. Paimon keeps its own option resolution;
explicit `orc.compress` overrides per-file compression. Shared filesystem rolling,
partition commit, naming and recovery are the same as the [Parquet sink](parquet.md#sink).
`streamfusion.operator.orcSink.enabled=false` disables the filesystem sink substitution;
Paimon's existing source and sink switches control its ORC paths.

| Writer option | Native support |
|---|---|
| `orc.compress` | NONE, ZLIB, SNAPPY, LZ4; also ZSTD for Paimon's newer Java reader |
| `orc.stripe.size`, `orc.compress.size` | Stripe and compression block sizes |
| `orc.row.index.stride` | Index stride; `0` disables indexes |
| `orc.dictionary.key.threshold` | Dictionary selection threshold |
| `orc.compression.strategy` | SPEED or COMPRESSION |
| `orc.write.format` | 0.11 or 0.12 |
| `orc.bloom.filter.columns`, `orc.bloom.filter.fpp` | Java-resolved column IDs and false-positive probability |
| Paimon `file.compression`, `file.block-size`, `file.compression.zstd-level` | Normal Paimon ORC mappings; ZSTD levels 1 and 3 |
| Paimon `orc.timestamp-ltz.legacy.type` | Both values in UTC; default remains Paimon's `true` |

Unsupported codecs (including LZO), other ZSTD levels, contradictory ZSTD level/strategy settings,
unknown options and non-default options without a C++ equivalent retain the stock writer.
Flink 2.2's ORC 1.5.6 reader does not understand ZSTD, so filesystem ZSTD stays stock. Untranslated
options are admitted only at the host default. The reader uses Java's configuration whitelist;
query filters remain residual Flink filters.

Paimon 2.0.0's patched Java writer and Flink's released ORC 1.5.6 writer ignore `orc.create.index`;
native writing preserves that behavior. Use `orc.row.index.stride = 0` to disable indexes.

Scalar booleans, integers, floating point, decimals through precision 38, strings, binary, dates
and timestamps, plus recursive arrays, maps and rows, use the host's ORC type converter.
CHAR/VARCHAR lengths and Paimon field IDs are preserved. CHAR reads remove trailing spaces like
Java ORC. Null parent structs and null lists with retained child spans are handled explicitly.
Paimon's type gate and snapshot-key gate are shared with Parquet, including the precision-6
timestamp limit and Java fallback for floating-point snapshot keys. Filesystem ORC retains the
released Flink converter's narrower type support.

Native timestamp paths require a UTC JVM timezone (`-Duser.timezone=UTC`). C++ and Java differ in
DST gap resolution and historical timezone rules; other JVM zones retain the stock path when the
schema contains timestamps. Historical NTZ files with non-UTC stripe writer timezones also retain
Java. Timestamp values share the engine's signed 64-bit nanosecond range, approximately 1677–2262.
The historical ORC last-negative-second encoding and its statistics follow Java's behavior.
That alias can make decoded timestamp keys unsorted. Native snapshot merging therefore retains
Java when a fractional timestamp is a non-leading stored key, or when a leading timestamp key's
file bounds intersect the final second before the Unix epoch. Decoding/tailing still preserves
Java's encoded values; post-epoch leading timestamp keys use the native merger.

Other filesystem fallbacks are unchanged from Parquet: updating inputs, overwrite,
auto-compaction, zero-column files, unsupported abilities, and sink constraints requiring stock
nullability or length enforcement. Paimon preserves its existing table-mode/configuration gates.

## Build and verification

Install `streamfusion-core`, `streamfusion-orc`, and Flink's normal `flink-orc` JAR for filesystem
sinks. Paimon supplies its own shaded Java ORC classes; install its connector, the Paimon module
and the ORC module, following [Paimon's factory ordering](paimon.md#deployment).
See [Deployment](../deployment.md#contributing-from-source) for the portable Maven/C++ build.

Run the untouched released Flink ORC SQL tests with:

```bash
bin/flink-suite.sh orc
```

The UTC harness passed all 46 cases in `OrcFsStreamingSinkITCase` and `OrcFileSystemITCase`,
including its required native-writer marker.
Local tests compare Java/native rows and footer statistics across codecs, sliced nested batches,
projection order, Paimon snapshot key types and streaming SQL. Shared file-writer harness tests
cover checkpoint visibility, partition directories and exactly-once restoration for both codecs.

The Paimon file-reader and changelog-writer diagnostics accept `SF_PAIMON_FILE_FORMAT=orc`;
run them with `mvn test -Pbench,paimon` so the native libraries are optimized. Their costs and
validation remain identical to the Parquet diagnostics documented on the Paimon page.

On local Apple Silicon with release libraries and mimalloc, the 262,144-row reader diagnostic
measured:

| Path | Java rows | Java to Arrow | Native to Arrow | Native / Java-to-Arrow throughput |
|---|---:|---:|---:|---:|
| Append file read | 0.027 s | 0.104 s | 0.057 s | 1.82× |
| Changelog file read | 0.020 s | 0.095 s | 0.047 s | 2.01× |

The 200,000-change writer diagnostic measured:

| Path | Java | Native | Throughput ratio |
|---|---:|---:|---:|
| Input changelog write | 0.567 s | 0.311 s | 1.82× |
| Lookup changelog write | 0.474 s | 0.368 s | 1.29× |
| Full-compaction changelog write | 0.314 s | 0.350 s | 0.90× |
| Deletion-vector write | 0.374 s | 0.273 s | 1.37× |

These are local diagnostics, not whole-job throughput claims. Native reading is slower than
Java's row checksum scan but faster than Java reading followed by the existing row-to-Arrow
converter. All paths project four columns, including nested data, but the checksum touches only
the first integer column. Both Arrow paths materialize every projected column; the Java row scan
does not construct Arrow output. The Java-to-Arrow baseline includes the production fallback's
row ownership copy and uses the same 4,096-row batch size and changelog sidecar as native reading.
The current ORC adapter appends values individually and allocates Arrow output for each batch.
Host FileIO callbacks and Arrow import also occur within the timed native path. These are known
additional operations, not a profile attributing the slowdown to any one of them. The Arrow
comparison measures the boundary needed by the native pipeline and snapshot merger; it does
not establish that the C++ codec alone decodes faster than Java, or that an entire Flink job does.
The writer includes row-to-Arrow conversion, routing, sorting, encoding,
and Paimon's checkpoint/compaction work; full compaction remains Java.

```bash
SF_PAIMON_FILE_FORMAT=orc SF_PAIMON_SOURCE_BENCHMARK=true SF_PAIMON_CHANGELOG_BENCHMARK=true \
  mvn test -Pbench,paimon -pl :streamfusion-paimon -am \
  -Dtest=PaimonSourceBenchmark,PaimonChangelogSinkBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false
```
