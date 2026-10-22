# Native date-partition Calc

## Query and implementation

This benchmark measures a synchronization projection with a nullable BIGINT epoch-second
column, zero, 16 or 107 passthrough STRING columns, and three date-partition fields:

```sql
SELECT *,
  SUBSTR(FROM_UNIXTIME(n, 'yyyyMMddHHmm'), 0, 8) AS ds,
  SUBSTR(FROM_UNIXTIME(n, 'yyyyMMddHHmm'), 9, 2) AS hh,
  LPAD(CAST((CAST(SUBSTR(FROM_UNIXTIME(n, 'yyyyMMddHHmm'), 11, 2) AS INT)
    / 15) * 15 AS STRING), 2, '0') AS mm
FROM src
```

The SQL is unchanged, including the substring positions and integer division. The local
time zone is `Asia/Shanghai`; legacy cast behavior is enabled. Every expression in this
projection runs in Rust: the benchmark rejects unexpected fallback or JVM function assignments.
The previous baseline is canonical main `d951a838`, where competing potentially failing
expressions cause this entire Calc to use the generated JVM evaluator.

The new planner retains independent expressions on their selected columnar paths instead of
moving the entire Calc to Java merely to choose the same first exception as Flink. Historical
regional offsets are serialized from the planning JDK's `java.util.TimeZone`, including its
pre-1900 raw offset and final standard-time entry. Rust selects an offset using the wrapped
millisecond timestamp, preserving Flink's Gregorian/Julian calendar behavior and overflow.
Formatting uses one scratch string per batch and reuses the result for consecutive equal
seconds, including equality across intervening NULLs. Passthrough columns share Arrow buffers.
Recurring future rules and unverified patterns keep the existing Flink evaluator.

## Configuration and timing boundaries

Measurements were collected on 2026-10-22 with an Apple M4 Pro (14 CPU cores, 24 GiB RAM),
macOS, JDK 11.0.27, and released Flink 2.2.1 and 1.18.1. Native code uses release/mimalloc,
`--no-default-features --features mimalloc`; this stateless Calc does not use RocksDB.
There is one processing thread, 131,072 logical rows and 1,024 rows per Arrow batch.
Epoch seconds are `1730000000 + (row / repeat) % 86400`, with NULL every nineteenth row.
STRING column `j` holds `field-j-value-(row % 127)`, with NULL when `(row + j) % 23 == 0`.
`repeat=1` varies each non-NULL timestamp; `repeat=64` models repeated timestamps.

Input preparation, planning and operator setup are untimed. Each shape/engine uses a fresh
process, five warmups and nine measured trials; engines run sequentially, without CPU pinning
or interleaving. Every trial additionally prewarms a 1,024-row prefix. Tables report medians
and full observed ranges, in milliseconds. These short local measurements should be repeated
on deployment hardware before capacity planning.

Two boundaries are deliberately reported separately:

- **Prepared functionality:** stock Flink's production generated Calc starts in Java on
  prebuilt BinaryRowData; StreamFusion's production Calc starts and stops its timer inside
  Rust on equivalent prepared Arrow batches. Both consume the three computed strings with
  the same byte checksum and verify the row count. Initial import, final export, perimeter
  JNI and row/Arrow transposes are excluded from the Rust timer. This is the small-functionality
  gate, not complete-job throughput.
- **Operator and discard:** the Java operator harness times the stock or native production
  Calc. Native timing includes JNI and Arrow import/export. Output is counted and discarded,
  releasing native batches as a columnar blackhole would. Stock starts on RowData; native
  starts on Arrow. This includes the Calc's wrapper but excludes sources, sinks, network,
  checkpoints, scheduling, and perimeter row/Arrow transposes. The old and new StreamFusion
  paths use this same boundary for the before/after comparison.

Prepared-functionality checksums match stock on every shape. The separate SQL parity suite
checks full results, historical transition boundaries, the long timestamp range, both cast
modes, NULLs, and all 107 passthrough columns across multiple batches. A discard measurement
is not itself a value-correctness check.

## Prepared functionality

| Flink | STRING columns | Repeat | Stock ms (range) | Rust ms (range) | Stock / Rust |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2.2.1 | 0 | 1 | 91.380 (85.793-114.689) | 33.841 (33.709-34.970) | 2.70x |
| 2.2.1 | 0 | 64 | 88.050 (86.508-118.483) | 10.591 (10.375-10.814) | 8.31x |
| 2.2.1 | 16 | 1 | 171.797 (148.993-184.011) | 35.612 (34.906-36.088) | 4.82x |
| 2.2.1 | 16 | 64 | 162.704 (148.047-198.751) | 11.126 (10.887-11.248) | 14.62x |
| 2.2.1 | 107 | 1 | 585.205 (549.459-755.492) | 36.227 (35.499-36.584) | 16.15x |
| 2.2.1 | 107 | 64 | 603.930 (565.010-941.474) | 11.169 (10.975-11.500) | 54.07x |
| 1.18.1 | 107 | 1 | 555.952 (549.158-576.984) | 35.379 (34.899-36.197) | 15.71x |
| 1.18.1 | 107 | 64 | 556.691 (540.476-579.176) | 11.190 (11.053-12.137) | 49.75x |

The width-dependent advantage includes avoiding row-by-row reconstruction of passthrough
columns; it is not solely a formatter speedup.

## Operator and discard

| Flink | STRING columns | Repeat | Stock ms (range) | Previous ms (range) | New native ms (range) | Stock / new | Previous / new |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2.2.1 | 0 | 1 | 83.249 (82.082-101.300) | 88.500 (85.290-108.670) | 35.360 (34.826-36.071) | 2.35x | 2.50x |
| 2.2.1 | 0 | 64 | 82.883 (77.860-99.555) | 73.191 (72.135-87.887) | 13.233 (12.308-21.388) | 6.26x | 5.53x |
| 2.2.1 | 107 | 1 | 566.814 (547.545-577.073) | 1487.594 (1473.739-1517.108) | 75.783 (74.914-85.993) | 7.48x | 19.63x |
| 2.2.1 | 107 | 64 | 564.488 (548.688-597.774) | 1552.953 (1535.647-1579.567) | 54.740 (51.015-61.114) | 10.31x | 28.37x |
| 1.18.1 | 107 | 1 | 433.493 (428.491-493.347) | 1555.459 (1517.231-1801.218) | 79.176 (75.626-88.230) | 5.48x | 19.65x |
| 1.18.1 | 107 | 64 | 574.308 (548.294-630.062) | 1602.348 (1565.736-1757.981) | 54.091 (52.008-71.324) | 10.62x | 29.62x |

The native wrapper has a visible cost on wide batches, but the measured operator still
outperforms both stock Flink and the prior whole-Calc JVM path. This evidence does not
measure remote connector throughput or establish an end-to-end production-job speedup.
Rowwise source/sink deployments must additionally pay their perimeter transposes.

[Raw measured trials](native-sync-calc-2026-10-22.csv) identify each timing boundary. Rows
marked `prepared-functionality` compare the Java stock driver with the Rust driver; rows
marked `operator-discard` compare all three Java operator drivers.

## Reproduce

Build release native code and export an admitted planner program with the Java driver:

```bash
cargo build --release --no-default-features --features mimalloc \
  --manifest-path native/Cargo.toml -p streamfusion --lib --example sync_date_partition

SF_BENCHMARK=true mvn -pl streamfusion-runtime -Pbench \
  -Dnative.build.skip=true -Dtest=SyncDatePartitionBenchmark \
  -Dsync.mode=stock -Dsync.width=107 -Dsync.repeat=1 \
  -Dsync.export=/tmp/sync-date.arrow test

native/target/release/examples/sync_date_partition /tmp/sync-date.arrow 5 9
```

Use `-Pbench,flink-1.18` for that release line. Defaults are 131,072 rows, five warmups,
nine measurements, and 1,024 rows per batch. The exported Arrow file has a companion
`.plan.json` produced by the real planner, not a hand-written native expression.
The Rust driver asserts that its program has no JVM expression kinds.

For operator timing, run the Java driver with `-Dsync.discard=true` and
`-Dsync.mode=stock` or `-Dsync.mode=native`. Repeat for widths 0/16/107 and repetitions
1/64. Keep the Rust driver's batch size at its fixed 1,024 rows when comparing these
measurements; `sync.batchRows` only changes the Java driver.

For a previous-version comparison, use a separate worktree at `d951a838`, copy only the
portable benchmark Java source into that worktree's test sources, build its own release
native library, and run the Java driver with `-Dsync.mode=previous -Dsync.discard=true`.
This mode permits the baseline JVM assignment; it does not change the engine's SQL
or evaluation path. Keep each Flink line's compiled target directory separate.

## Correctness evidence

The JDK 11 regression selection passes on both supported Flink lines: competing failures,
empty/all-filtered batches, untaken branches, shared scalar UDFs, processing-time expressions,
IFNULL, guarded division and basic casts. The final additional selection passes 170 tests on
2.2.1 and 104 on 1.18.1, with 66 existing release/feature exclusions on 1.18.1. These additional
tests cover the complete date projection, every passthrough column, collection representations,
array edits/extrema, MAP_UNION, fixed binary and dynamic charset behavior. Rust's core suite
passes all 759 tests with the measured release/mimalloc feature selection. A JDK 17 control
also passes the formatter's offset/admission, SQL parity and independent-cast routing tests.

The unchanged released `TemporalTypesTest.testFromUnixTime` and `testFromUnixTimeInTokyo`
pass against both Flink lines on JDK 17. They validate Flink's expression behavior but do not
establish native execution. The unchanged released `CalcITCase.testLongProjectionList` and
`testIfFunction` pass in stock and injected-native runs on both lines. Their existing native
contracts record three and six consumed rows respectively, with no fallback. The new SQL
date-projection tests independently require successful native consumption and `jvmFunctions=[]`.
