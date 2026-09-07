# Apache Paimon

**Status:** experimental. The optional `streamfusion-paimon` module accelerates streaming
`INSERT INTO` jobs into Paimon **append-only tables** on the published Paimon `2.0.0` Flink 2.2
connector. Paimon keeps every table-level responsibility: schema and catalog, bucket assignment
rules, sequence numbers, file rolling, statistics, manifests, snapshots, commits, and the in-job
compaction topology. StreamFusion replaces two things: the per-row shuffle in front of the writers
and the Parquet encoding of each data file.

## What runs natively

A sink's Arrow batches are split natively into one sub-batch per `(partition, bucket)` pair. Rust
computes the partition `BinaryRow` and Paimon's default bucket hash column-wise (Paimon's row
layout and Murmur hash are Flink's, so the native key encoder already produces both), and the
routed batches are shuffled while still Arrow with Paimon's own channel formula. Each batch then
enters Paimon's bundle write entry point for its bucket and reaches a StreamFusion
`FileFormatFactory` registered under the `parquet` identifier, whose writer encodes the whole batch
with the standard parquet-rs `ArrowWriter` over Paimon's output stream. Paimon reads statistics
from the resulting footer exactly as from its own files.

Supported:

- Bucket-unaware tables (`bucket = -1`), partitioned or not, with `partition.sink-strategy` `none`
  or `hash`, including Paimon's in-job compaction coordinator and workers.
- Fixed-bucket append tables (`bucket > 0` with `bucket-key`), partitioned or not, with the
  `sink.parallelism` and small-bucket-count parallelism rules of the stock sink.
- `file.format = parquet` with `file.compression` `none`/`snappy`/`gzip`/`zstd` (and
  `file.compression.zstd-level`), `file.block-size`, and the `parquet.*` writer keys the stock
  writer honours: page and dictionary-page size, dictionary encoding, writer version.
- Column types `BOOLEAN`, `TINYINT`..`BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`, `CHAR`/`VARCHAR`,
  `BINARY`/`VARBINARY`, `DATE`, `TIMESTAMP`/`TIMESTAMP_LTZ` up to precision 6, and `ARRAY`, `MAP`,
  `ROW` of those, recursively, with Paimon's field ids on every column.
- Hint options (`/*+ OPTIONS(...) */`) and the `paimon.<catalog>.<db>.<table>.<option>` dynamic
  options from the job configuration, resolved the way Paimon's own factory resolves them.
- `sink.writer-refresh-detectors`: the writer re-reads the refreshed option groups (external data
  paths) after each checkpoint's commit preparation, exactly when the stock operator does.

Files written natively are row-, statistics-, and footer-schema-identical to the stock writer's
(verified against twin tables in `PaimonSinkParityTest` and `NativePaimonParquetWriterTest`). The
one known statistics difference: a `DOUBLE`/`FLOAT` column whose minimum is a negative zero is
recorded as `-0.0` by parquet-rs and `0.0` by parquet-mr.

## Falls back to stock Paimon on

Each of these declines at planning time with a reason visible in `NativePlanner.explain`:

- Primary-key tables (see the outlook below), dynamic-bucket and postpone-bucket modes.
- A changelog (retracting or updating) input, `INSERT OVERWRITE`, and batch-mode inserts (the
  substitution only exists in the streaming planner).
- `file.format` other than `parquet`, `file.format.per.level`, `write-buffer-for-append = true`,
  file indexes (`file-index.*`), `row-tracking.enabled`, `data-evolution.enabled`, `BLOB` columns.
- `sink.clustering.*`, `partition.sink-strategy = PARTITION_DYNAMIC`,
  `sink.writer-coordinator.enabled`, `sink.coordinator-commit.enabled`.
- `TIMESTAMP` precision above 6 (Paimon writes INT96 there), `VARIANT`, vector, and geospatial
  types.
- `parquet.*` keys the native writer cannot honour: bloom filters, page validation, custom
  padding, page row-count limits, statistics/column-index truncation, page-size row checks,
  multithreaded zstd, and any per-column (`parquet.*#column`) or unrecognised writer key.
- The `parquet` format identifier resolving to Paimon's own factory (see deployment below).

Two runtime situations route rows through Paimon's stock Parquet writer inside an otherwise native
job, keeping the output identical to stock Paimon at the cost of the native speed-up for those
files: compaction rewrites (in-job or from a dedicated compaction job), and Paimon's buffer-spill
mode, which a writer task enters once it holds more than `write-max-writers-to-spill` (default 10)
partition-bucket writers and which re-buffers and rewrites what those writers had already written.
In spill mode the absolute sequence numbers in file metadata differ from a stock run (Paimon
reassigns them on the rewrite, and it triggers after whole routed batches rather than single rows);
rows, statistics, and footers are unchanged.

## Deployment

Install the published `paimon-flink-2.2-2.0.0.jar`, `streamfusion-parquet`, and
`streamfusion-paimon` in Flink's `lib/`. Paimon resolves a file format by taking the **first**
`FileFormatFactory` on the classpath that claims the identifier, and Flink adds `lib/` JARs in
sorted name order, so the StreamFusion JAR must sort before `paimon-flink-*`: name it
`01-streamfusion-paimon.jar` (the same convention as `00-streamfusion-loader.jar`). The planner
checks at planning time that `parquet` resolves to the StreamFusion factory and declines the sink
with an explicit reason otherwise; it never enters the native topology only to discover the stock
format at runtime. Removing the StreamFusion Paimon JAR restores the stock connector entirely.

Batch inserts, compaction jobs, and any other row-fed path see the same factory: for rows it
delegates each file to Paimon's own writer, so a mixed deployment stays byte-compatible with stock
Paimon.

## Outlook

Primary-key tables (merge-on-read, copy-on-write, and deletion-vector merge-on-write), dynamic
buckets, ORC data files, the writer and commit coordinators, and clustering are tracked in
[issue #27](https://github.com/datafusion-contrib/StreamFusion/issues/27). Released Paimon 2.0.0
walks a bundle row by row before the format writer; a Paimon release that passes bundles through
takes the same writer's direct path with no change here. The jar-ordering requirement goes away
once Paimon's format discovery gains a priority, which is proposed upstream.

Build with the `paimon` Maven profile. The module has no snapshot, local-Maven, path, or forked
Paimon dependency.
