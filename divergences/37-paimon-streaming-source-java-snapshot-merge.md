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
For this boundary, the already-released parquet-rs dependency is sufficient; no paimon-rust source
or dependency is imported. Java merges the initial primary-key snapshot and those logical rows
are converted to Arrow. The agreed follow-up is
[#53](https://github.com/datafusion-contrib/StreamFusion/issues/53), which will adapt paimon-rust's
snapshot merge without taking over the Java table lifecycle. Batch SQL remains out of scope.

The source's native file path is restricted to the current schema without deletion-vector
selections. Historical schemas, deletion vectors, historical non-Parquet files and specialized
split representations retain Java's table reader and conversion. This is an explicit hybrid read
contract, not a late failure on an unsupported file. Query filters stay as Flink residuals; no
filter is silently consumed. Advanced source options and abilities decline at planning time.

The connector coverage page describes the exact gates, memory boundary, validation and release
file-reader diagnostic (1.30x append and 2.18x changelog throughput on the local fixture).
