# ORC

**Status:** experimental, partial. The optional `streamfusion-orc` module supplies native ORC
encoding for streaming filesystem sinks and native decoding/encoding for [Paimon](paimon.md).
Batch jobs retain the stock connector.

## Architecture

Flink/Paimon keep file discovery, filesystem plugins, credentials, partition and bucket routing
rules, commits, checkpoints, compaction and statistics extraction. Production reads use released
`orc-rust` 0.9.0. Its Arrow 59 output crosses the standard C Data ABI into our Arrow 58 operators
without copying buffers. Projection order and logical types are restored, and CHAR values are
trimmed recursively in arrays, maps and rows to match Java. Apache ORC C++ still writes ORC
vectors through the nanoarrow adapter. Java owns the seekable input and recoverable output
streams, with bounded JNI transfers. ORC, Parquet and the engine remain separate native libraries.

Filesystem source scans stay on stock Flink, as with Parquet. Paimon streaming sources use native
Arrow decoding for append files and changelog files, and the existing native merger for admitted
primary-key snapshots. The same Java split planning, row-kind handling, projection, restore offsets
and fallback rules apply to both formats. ORC footers are inspected before a split emits data.
LZO files, timestamp files written in an unverified timezone, encrypted files, and files without enough payload or collection-count statistics to estimate
decoder memory retain Java reading. Rust checks all stripe footers before emitting data.
Snapshot admission accounts for retained compressed streams, decompression scratch space and
conservative whole-stripe decoded column/dictionary bounds, including null collection elements.
Declared compression block sizes are upper bounds; tiny streams are bounded by their decoded
column sizes plus encoding overhead. This is not a hard Flink managed-memory reservation.

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
additional operations, not a profile attributing the slowdown to any one of them. This older
diagnostic imports native output into **Java Arrow** before checksumming it; its endpoint differs
from consumption by our Rust operators. It does not establish that the C++ codec alone decodes
faster than Java, or that an entire Flink job does.
The writer includes row-to-Arrow conversion, routing, sorting, encoding,
and Paimon's checkpoint/compaction work; full compaction remains Java.

```bash
SF_PAIMON_FILE_FORMAT=orc SF_PAIMON_SOURCE_BENCHMARK=true SF_PAIMON_CHANGELOG_BENCHMARK=true \
  mvn test -Pbench,paimon -pl :streamfusion-paimon -am \
  -Dtest=PaimonSourceBenchmark,PaimonChangelogSinkBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false
```

### Reading into arrow-rs

`OrcReaderComparisonBenchmark` compares four paths through consumption of an Arrow Rust 58
`RecordBatch`: our ORC C++/nanoarrow adapter, Arrow C++ `ORCFileReader`, `orc-rust`, and Paimon's
Java reader followed by the production row ownership copy, row-to-Arrow conversion and native
C Data import. Native batches never return to Java in this measurement. Java owns file I/O for
all readers; the three native decoders use the same synchronous seek/read callback and 64 KiB
copy buffer. The timer includes file opening, decoding, projection, conversion to the operator
schema, a common Rust integer checksum, and reader/batch destruction. Planning and writing
fixtures are outside timing. Every projected column is materialized; the timed checksum touches
the first integer column. Separate untimed scans compare order-independent full-row fingerprints
with Java.

The opt-in build uses released Arrow C++ 25.0.1 and orc-rust 0.9.0. Both C++ adapters link the
same ORC 2.3.1 library. The build compiles the unchanged released Arrow ORC adapter sources
against that library, avoiding two copies of ORC/protobuf. orc-rust uses Arrow Rust 59, so its
output crosses the standard Arrow C Data ABI into our existing Arrow Rust 58 without copying
the buffers. Any required schema casts are included in timing. All native paths use release
optimization and the module's mimalloc allocator.

```bash
SF_ORC_READER_COMPARISON=true \
  mvn test -Pbench,paimon,orc-reader-bench -pl :streamfusion-paimon -am \
  -Dtest=OrcReaderComparisonBenchmark -Dsurefire.failIfNoSpecifiedTests=false
```

`SF_ORC_COMPARISON_ROWS` defaults to 262144, `SF_ORC_COMPARISON_TRIALS` to five, and
`SF_ORC_COMPARISON_CODECS` to `none,zstd` (also accepts `zlib,snappy,lz4`). Each case rotates the
four readers through two warmups and the measured trials, reporting the median and minimum.
Files are local and warm in the OS cache. `peak_batch_bytes` is Arrow's reported memory size of
the largest output batch, **not peak decoder or process memory**. `io_bytes` counts bytes
requested through the native readers' host callback; it is unavailable for the Java baseline.

The C++ comparison adapters and diagnostic JNI entry points are enabled by `orc-reader-bench` /
`reader-comparison`. Production reads use the separate orc-rust integration with metadata
admission and recursive CHAR normalization. The comparison profile is for tests, not packaging.

On an Apple M1 Max (64 GiB RAM, Java 17, UTC), five-trial median times for 262,144 rows were:

| Codec | Columns / file mode | Our adapter | Arrow C++ | orc-rust | Java to arrow-rs |
|---|---|---:|---:|---:|---:|
| NONE | 4 / append | 45.0 ms | 19.4 ms | 18.0 ms | 114.5 ms |
| NONE | 14 / append | 141.8 ms | 63.8 ms | 56.8 ms | 343.8 ms |
| NONE | 4 + row kind / changelog | 37.4 ms | 12.3 ms | 10.7 ms | 104.7 ms |
| ZLIB | 4 / append | 55.6 ms | 30.1 ms | 29.5 ms | 122.9 ms |
| ZLIB | 14 / append | 165.5 ms | 87.5 ms | 84.0 ms | 365.1 ms |
| ZLIB | 4 + row kind / changelog | 44.7 ms | 19.7 ms | 17.9 ms | 111.7 ms |
| SNAPPY | 4 / append | 44.7 ms | 19.4 ms | 18.3 ms | 111.9 ms |
| SNAPPY | 14 / append | 145.2 ms | 67.5 ms | 62.1 ms | 354.8 ms |
| SNAPPY | 4 + row kind / changelog | 38.4 ms | 13.4 ms | 11.7 ms | 106.6 ms |
| LZ4 | 4 / append | 44.4 ms | 19.3 ms | 17.6 ms | 107.6 ms |
| LZ4 | 14 / append | 143.0 ms | 66.0 ms | 58.7 ms | 342.8 ms |
| LZ4 | 4 + row kind / changelog | 37.9 ms | 12.8 ms | 10.7 ms | 104.3 ms |
| ZSTD | 4 / append | 47.2 ms | 23.3 ms | 20.9 ms | 110.8 ms |
| ZSTD | 14 / append | 149.1 ms | 72.9 ms | 64.5 ms | 346.7 ms |
| ZSTD | 4 + row kind / changelog | 39.2 ms | 14.3 ms | 12.4 ms | 104.5 ms |

[Detailed measurements](../benchmarks/orc-readers-2026-09-14.csv) include minimum times, reported
output-batch memory and host I/O bytes. The widest output batch reports 807,087 bytes on all four
paths; this does not rank their internal allocation peaks. These measurements do not include
snapshot merging, Flink scheduling, checkpoints, or remote storage.

Arrow C++ is 1.85–3.03× faster than our adapter, and orc-rust is 1.88–3.53× faster. orc-rust leads
Arrow C++ by 2–19% in this sample. That supports evaluating orc-rust as the replacement reader;
Arrow C++ is also a substantial improvement. There is no architectural requirement to avoid an
Arrow C++ dependency. Its portable source build works here, at the cost of another C++ dependency.

The candidates match Java on these timed datasets and on 21 scalar edge-case fixtures including
integer limits, floating-point NaNs/infinities/signed zero, decimal precision 38, Unicode strings,
binary, historical dates and fractional pre-epoch timestamps. The raw-reader fixture exposes a
specific integration difference: **both candidates return padded CHAR strings**, whereas Java and
our adapter strip trailing spaces. The raw comparison asserts the padded result separately. Production orc-rust reads now normalize
CHAR recursively and pass the existing source admission, timezone, recovery and snapshot suites.
The production reader switch passed 216 focused tests, including 136 snapshot-key cases,
40 scalar-value cases, four nested-CHAR cases and source/ORC regressions. Arrow C++ remains only
a benchmark candidate. Linux comparison-profile builds and peak decoder memory still need
separate verification.

### Writing from arrow-rs

`OrcWriterComparisonBenchmark` compares our standard ORC C++ 2.3.1 writer and nanoarrow
adapter with Paimon 2.0.0's shaded ORC Java 1.9.8 vectorized writer. Both start from the same
Rust-owned Arrow 58 batches and end with a closed local ORC file. Fixture generation and its
one-time copy into Rust-owned buffers happen before timing; the fixture's Java allocator is
closed before either writer runs. The Java candidate imports those buffers through C Data and
copies columns directly into reusable Hive vectors. Long/double columns and string/binary
payloads use bulk copies; decimals through precision 18 use ORC’s `decimal64` vectors and
larger decimals use reusable binary scratch space; nested columns recurse.
There are no intermediate `RowData` objects in either timed path.

Timing includes opening the output, constructing the encoder, every batch conversion, encoding,
footer/stripe flushes, output close and resource cleanup. Both use Paimon's local `FileIO`;
C++ drains through the production 1 MiB JNI output buffer. Each uses 64 MiB stripes, 64 KiB
compression buffers, row indexes every 10,000 rows, dictionary threshold 0.8, compression
strategy SPEED by default, and ORC 0.12. Different ORC implementations can choose different encodings and
stripe boundaries despite matching settings, so the harness also reports file size. Output
close does not include `fsync`; these are local filesystem/OS-cache measurements.

Before timing, a third, untimed path writes the original fixture rows using Paimon's stock
`RowDataVectorizer`. Paimon's Java ORC reader compares ordered fingerprints of every column
against both candidates, and checks row counts, schema, field IDs, codec, format version and
index stride. It checks the final timed outputs again. Scalar compatibility cases add integer
extremes, floating special values, decimal precision 38, CHAR/VARCHAR, binary and pre-epoch
microsecond timestamps. This is a writer microbenchmark, not a Paimon checkpoint or Flink job
benchmark, and does not change the deployed writer.

```bash
SF_ORC_WRITER_COMPARISON=true \
  mvn test -Pbench,paimon,orc-writer-bench -pl :streamfusion-paimon -am \
  -Dtest=OrcWriterComparisonBenchmark -Dsurefire.failIfNoSpecifiedTests=false
```

The test-only `orc-writer-bench` profile enables the native `writer-comparison` fixture helpers;
it does not add Arrow C++ or orc-rust dependencies. Run it separately from `orc-reader-bench`.
`SF_ORC_WRITER_ROWS` defaults to 262144, `SF_ORC_WRITER_BATCH_ROWS` to 4096,
`SF_ORC_WRITER_TRIALS` to five and `SF_ORC_WRITER_WARMUPS` to two. The candidates alternate
execution order. `SF_ORC_WRITER_CODECS` defaults to `NONE,ZLIB,SNAPPY,LZ4,ZSTD`;
`SF_ORC_WRITER_STRATEGY` accepts `SPEED` (default) or `COMPRESSION`. Numeric,
string/binary and full nested Paimon schemas use four, four and fourteen columns respectively.

On an Apple M1 Max (64 GiB, Java 17 with an 8 GiB heap, UTC), release/mimalloc,
five-trial median times for 262,144 rows were:

| Schema | Codec / strategy | C++ | Java vectors | Java time / C++ time |
|---|---|---:|---:|---:|
| Numeric, 4 columns | NONE / SPEED | 12.9 ms | 20.0 ms | 1.55× |
| Strings/binary, 4 columns | NONE / SPEED | 28.8 ms | 49.0 ms | 1.70× |
| Nested Paimon, 14 columns | NONE / SPEED | 189.1 ms | 172.1 ms | 0.91× |
| Numeric, 4 columns | ZSTD / SPEED | 16.3 ms | 21.7 ms | 1.33× |
| Strings/binary, 4 columns | ZSTD / SPEED | 36.0 ms | 57.2 ms | 1.59× |
| Nested Paimon, 14 columns | ZSTD / SPEED | 207.6 ms | 198.1 ms | 0.95× |

Java is competitive on this wider schema: across NONE, ZLIB, SNAPPY and ZSTD its median
ranges from effectively tied to 9% less elapsed time. C++ wins on both narrow schemas, where
Java takes 33–70% more time. This supports Java vectorized writing as a deployment tradeoff,
without establishing a universal writer ranking. The experiment does not measure the two
adapters separately from encoding, and the synthetic fixtures do not represent every data
distribution. A Java writer would remove the ORC C++ writer build; removing the ORC C++ library
entirely would also require replacing the deployed reader. Java's packaged compression
libraries can still contain native code.

[Complete results](../benchmarks/orc-writers-2026-09-14.csv) include all five codecs, file sizes,
minimum/maximum times and a separate LZ4 COMPRESSION run. LZ4 SPEED is a particularly different
tradeoff between the implementations: ORC C++ 2.3.1 chooses acceleration 65537, producing almost
uncompressed output on these fixtures. For the full schema it writes 15.41 MB versus Java's
7.48 MB. Its SPEED timing should therefore not be read as equal compression work. The
compression-oriented follow-up is reported separately below.

| Schema | LZ4 COMPRESSION, C++ | LZ4 COMPRESSION, Java | C++ file | Java file |
|---|---:|---:|---:|---:|
| Numeric, 4 columns | 14.8 ms | 23.6 ms | 0.918 MB | 0.918 MB |
| Strings/binary, 4 columns | 34.0 ms | 59.2 ms | 1.90 MB | 1.94 MB |
| Nested Paimon, 14 columns | 197.1 ms | 191.4 ms | 7.25 MB | 7.48 MB |

The main run passed all 24 comparison/type cases plus 17 existing ORC file and timestamp
regressions. The separate LZ4 run also passed its stock-Java comparisons. Neither timing
includes correctness rereads. These results apply to this machine and warmed process; they
do not measure concurrent writers, checkpoint latency or peak heap/native memory.
