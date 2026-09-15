# Java Paimon source lifecycle with native file decode

The first streaming source retains Java Paimon's ContinuousFileStoreSource enumerator, discovery,
assignment and checkpoint serializers. It replaces the task-side reader and emission type with
Arrow batches, following StreamFusion's Kafka source boundary. Released Paimon 2.0.0's
FileStoreSourceSplitReader, FileStoreSourceReader, MergeFileSplitRead and KeyValueTableRead were
consulted before implementation. Streaming primary-key splits use Java's no-merge file path;
initial snapshot splits may need sorted-run merging.

Arroyo's filesystem source already reads ParquetRecordBatch streams into its collector and keeps
file progress separately. We retain that columnar structure, but leave file enumeration and
logical-row checkpoint positions with Paimon's Flink source contract. Restores can change batch
size, so positions count emitted rows rather than decoded batches. Independent bucket splits may
reorder after restoration; the order of changes within each key is preserved.

Comet's NativeUtil C Data imports informed ownership and failure cleanup. The native synchronous
Parquet reader requests byte ranges through Paimon's seekable FileIO. A reusable Java 64 KiB array
feeds a borrowed direct buffer during the JNI call. No second storage client or credential stack
is introduced. Exported Arrow buffers transfer to the source batch; its existing abandoned-batch
backstop covers records discarded during task failure.

paimon-rust's table/data_file_reader.rs, table/kv_file_reader.rs and arrow/format/parquet.rs were
reviewed. They couple Parquet decode with their own schema manager, filesystem and read plans.
For file decoding, the already-released parquet-rs dependency is sufficient; no paimon-rust
filesystem or table-reader dependency is imported. Java merges unsupported initial primary-key
snapshot combinations and those logical rows are converted to Arrow. The first native
snapshot merger adapts paimon-rust's Arrow cursor/output algorithm and partial-update cell
selection into the optional Paimon library.
[#53](https://github.com/datafusion-contrib/StreamFusion/issues/53) tracks broader snapshot merge
coverage while retaining the Java table lifecycle. Batch SQL remains out of scope.

The source's native file path is restricted to the current schema without deletion-vector
selections. Historical schemas, deletion vectors, historical non-Parquet files and specialized
split representations retain Java's table reader and conversion. This is an explicit hybrid read
contract, not a late failure on an unsupported file. Query filters stay as Flink residuals; no
filter is silently consumed. Advanced source options and abilities decline at planning time.

The connector coverage page describes the exact gates, memory boundary, validation and release
file-reader diagnostic (1.30x append and 2.18x changelog throughput on the local fixture).

Native snapshot sections and sorted runs are planned by released Java `IntervalPartition`.
A Java callback forwards Arrow C structs between the Parquet and Paimon libraries without a
row conversion or a cross-library Rust handle. Cursors and output gathering use synchronous batch
pulls and incremental winner retention. Completed batches can be reclaimed even when many keys
produce no output. The tree ports released Java's leaf states and tie traversal because paimon-rust's
simpler traversal selects different winners on exact sequence ties. Java also supplies partial-update
delete and singleton-reducer semantics; paimon-rust supplies per-column cell selection. We gather
those references by output batch instead of materializing a one-row batch for every key. Sequence
groups and field aggregates still retain Java. Raw-convertible snapshots emit inserts; selection
merges preserve the winning add kind, and multi-record partial reductions emit inserts or remove
the key. Recovery counts emitted merged rows and replays
the prefix, including when switching between Java and native readers. Apache attribution ships
in the Paimon JAR's NOTICE. Current coverage, budgets and catch-up measurements are in the
connector page.

Finite-file completion flushes the split's final bounded watermark before releasing its Flink
source output. Flink 2.2.1 removes a split's periodic generator in `releaseOutputForSplit`; an
Arrow reader can prefetch EOF and release a small file before that generator's next tick. The
three-row snapshot/one-row follow-up regression reproduced a stalled second window while stock
Paimon advanced. The completion flush uses only the maximum timestamp already emitted by that
split, minus the admitted watermark delay. It passes through Flink's existing split multiplexer,
retaining its handling of other active splits and idleness. Active files still emit periodically;
no watermark is inserted inside an Arrow batch. This is an explicit final-progress flush at file
completion, rather than relying on the stock reader's slower iteration and single-batch pool to
leave time for a periodic callback. Shared native window branches and stock Flink produce the same
results across both commits for Parquet and ORC.
