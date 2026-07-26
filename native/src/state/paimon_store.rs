//! Persistent state on local Apache Paimon primary-key tables: a write buffer over a disk table,
//! nothing else.
//!
//! The store holds exactly two components. The **write buffer** is the in-memory map of every
//! entry written since the last checkpoint barrier (upserts and removals); it answers reads for
//! those keys directly and is the only state that survives across batches. The **disk table** is
//! the committed Paimon snapshot, immutable between barriers. Each processed batch resolves its
//! reads with one point-read join: the batch's keys not already in the write buffer are pushed
//! into the table reader as an exact `IN` predicate (file/page stats prune, then a single
//! hash-set pass filters rows at parquet decode), and the matched rows live only until the end
//! of the batch's bundle — there is no retained cache of clean rows between bundles; re-reads
//! are served by the OS page cache plus decode, not by a second copy of the state in memory.
//!
//! At a checkpoint barrier the write buffer is encoded as one Arrow batch (`_VALUE_KIND` carries
//! upsert vs delete per row), committed, and cleared — a Paimon snapshot is a manifest-pinned
//! immutable file set, so durability lands exactly at checkpoints and "new files since the last
//! checkpoint" is a manifest diff, which is what makes Flink incremental checkpoints possible
//! upstream of this module.
//!
//! The table carries a computed `kg` INT column (`flink_key_group` of the BinaryRow key bytes) as
//! the leading primary-key column, so files' row groups are key-group-clustered, but the bucket
//! count is deliberately small and decoupled from max parallelism (default 1: one LSM per
//! subtask, the RocksDB shape). Rescale clips by key-group range at recovery time
//! (`clip_from_sources`); an aligned restore adopts the files wholesale.
//!
//! This store never compacts. paimon-rust has no LSM compaction yet, and rather than carry a
//! second maintenance implementation, table maintenance belongs exclusively to the optional Java
//! Paimon compactor module, which runs stock Paimon's compaction against this table at each
//! barrier, directly beneath the data commit (the store adopts its snapshots by re-pinning at
//! checkpoint start). Without it, tables stay correct but accumulate one level-0 run per touched
//! bucket per checkpoint — the host warns when the backend runs unmaintained.

use crate::state::dirty_region::DirtyRegion;
use crate::*;
use arrow::array::{Array, BinaryArray, Int32Array, Int8Array};
use paimon::catalog::Identifier;
use paimon::io::FileIO;
use paimon::spec::{
    BigIntType, BooleanType, DataField, DataType as PaimonType, Datum, DateType,
    DecimalType, DoubleType, FloatType, IntType, Predicate, PredicateBuilder,
    Schema as PaimonSchema, SmallIntType, TableSchema, TimestampType, TinyIntType,
    VarBinaryType, VarCharType, EMPTY_SERIALIZED_ROW,
};
use paimon::table::{CommitMessage, DataSplit, Table};
use std::collections::HashSet as StdHashSet;
use std::sync::OnceLock;

const KG_COLUMN: &str = "kg";
const KEY_COLUMN: &str = "k";
const VALUE_KIND_COLUMN: &str = "_VALUE_KIND";

/// The per-operator half of the store: the value columns beyond `kg`/`k`, and how one state value
/// maps to and from one row of those columns. The store owns keys, buckets, hydration, dirty
/// tracking, and the checkpoint file protocol; a codec owns only its row shape, so a new operator
/// plugs in with a schema fragment and a scalar round-trip.
pub(crate) trait PaimonStateCodec {
    type Value;

    /// Whether this operator instance's state shape is persistable at all (type map coverage,
    /// operator-specific restrictions). False keeps the operator on the memory backend.
    fn supported(&self) -> bool;

    /// The value columns beyond `kg`/`k`, in persisted order. All are stored nullable — a
    /// tombstone row carries nulls.
    fn value_fields(&self) -> Vec<(String, DataType)>;

    /// Encodes a value as one scalar per value column, in `value_fields` order.
    fn encode(&self, value: &Self::Value) -> Vec<ScalarValue>;

    /// Decodes one probe row (one scalar per value column) — the inverse of `encode`.
    fn decode(&self, scalars: &[ScalarValue]) -> Self::Value;

    /// The value's accounted heap footprint, mirroring the operator's own per-row tracking.
    fn value_bytes(&self, value: &Self::Value) -> usize;
}

/// One shared runtime for all Paimon state IO: probes and commits run on the Flink task thread via
/// `block_on`, so the runtime only needs to drive opendal's local-fs operations.
fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("paimon-state-io")
            .enable_all()
            .build()
            .expect("paimon state runtime")
    })
}

fn pe(e: paimon::Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// A probe column cast to the row codec's expected Arrow type when the file format decoded it as
/// a different (compatible) representation, e.g. a binary view.
fn normalized_column(
    batch: &RecordBatch,
    index: usize,
    expected: &Field,
) -> Result<ArrayRef, DataFusionError> {
    let column = batch.column(index);
    if column.data_type() == expected.data_type() {
        Ok(column.clone())
    } else {
        arrow::compute::cast(column, expected.data_type())
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

fn io(e: std::io::Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// The subset of Arrow state/key types this backend persists. Anything outside it (and any
/// multiset-backed aggregate) keeps the memory backend — a per-operator fallback, never an error
/// at runtime.
fn paimon_type_of(dt: &DataType) -> Option<PaimonType> {
    Some(match dt {
        DataType::Boolean => PaimonType::Boolean(BooleanType::new()),
        DataType::Int8 => PaimonType::TinyInt(TinyIntType::new()),
        DataType::Int16 => PaimonType::SmallInt(SmallIntType::new()),
        DataType::Int32 => PaimonType::Int(IntType::new()),
        DataType::Int64 => PaimonType::BigInt(BigIntType::new()),
        DataType::Float32 => PaimonType::Float(FloatType::new()),
        DataType::Float64 => PaimonType::Double(DoubleType::new()),
        DataType::Utf8 => PaimonType::VarChar(VarCharType::string_type()),
        DataType::Binary => {
            PaimonType::VarBinary(VarBinaryType::try_new(true, VarBinaryType::MAX_LENGTH).ok()?)
        }
        DataType::Date32 => PaimonType::Date(DateType::new()),
        DataType::Decimal128(p, s) if *s >= 0 => {
            PaimonType::Decimal(DecimalType::new(*p as u32, *s as u32).ok()?)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None) => {
            PaimonType::Timestamp(TimestampType::new(3).ok()?)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            PaimonType::Timestamp(TimestampType::new(6).ok()?)
        }
        // The host bridge pins every Flink TIMESTAMP/TIMESTAMP_LTZ column to nanoseconds with no
        // zone, so this arm is what row payloads carrying a rowtime column actually hit.
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
            PaimonType::Timestamp(TimestampType::new(9).ok()?)
        }
        _ => return None,
    })
}

/// True when every listed column type is persistable by this backend's type map.
pub(crate) fn paimon_row_supported(types: &[DataType]) -> bool {
    types.iter().all(|t| paimon_type_of(t).is_some())
}

/// The shared half of every row-payload codec (keep-last dedup, changelog normalize): the
/// persisted value IS the operator's stored full row as typed columns — never the transient
/// arrow-row bytes, mirroring the raw keyed-state snapshots (arrow-row encoding is not a stable
/// wire format). A side effect worth having: the state table reads like the operator's output
/// table itself.
pub(crate) struct RowPayloadCodec {
    row_types: Vec<DataType>,
    converter: arrow::row::RowConverter,
}

impl RowPayloadCodec {
    pub(crate) fn new(row_types: Vec<DataType>) -> Self {
        let converter = arrow::row::RowConverter::new(
            row_types.iter().map(|t| arrow::row::SortField::new(t.clone())).collect(),
        )
        .expect("row payload codec converter");
        RowPayloadCodec { row_types, converter }
    }

    pub(crate) fn supported(&self) -> bool {
        paimon_row_supported(&self.row_types)
    }

    pub(crate) fn fields(&self) -> Vec<(String, DataType)> {
        self.row_types
            .iter()
            .enumerate()
            .map(|(i, t)| (format!("c{i}"), t.clone()))
            .collect()
    }

    pub(crate) fn encode_payload(&self, payload: &[u8]) -> Vec<ScalarValue> {
        let parser = self.converter.parser();
        let columns = self
            .converter
            .convert_rows([parser.parse(payload)])
            .expect("decode row payload for persistence");
        columns
            .iter()
            .map(|column| ScalarValue::try_from_array(column, 0).expect("row payload scalar"))
            .collect()
    }

    /// Rebuilds the one-row typed columns and the arrow-row payload from a persisted row. The
    /// columns come back too so a codec can derive extra state from them (dedup's rowtime).
    pub(crate) fn decode_payload(&self, scalars: &[ScalarValue]) -> (Arc<[u8]>, Vec<ArrayRef>) {
        let columns: Vec<ArrayRef> = scalars
            .iter()
            .zip(&self.row_types)
            .map(|(scalar, data_type)| scalars_to_array(vec![scalar.clone()], data_type))
            .collect();
        let rows = self.converter.convert_columns(&columns).expect("encode hydrated row payload");
        (Arc::from(rows.row(0).data()), columns)
    }
}

/// True when every aggregate state column (and by construction the row codec) is persistable.
pub(crate) fn paimon_group_supported(kinds: &[i64], state_types: &[DataType]) -> bool {
    group_kinds_persistable(kinds) && paimon_row_supported(state_types)
}

pub(crate) struct PaimonStoreConfig {
    /// Absolute local directory holding this operator subtask's table (chosen by the host).
    pub table_dir: String,
    /// Flink maxParallelism — the modulus of the key-group column (`kg = hash mod this`).
    pub max_parallelism: usize,
    /// The table's Paimon bucket count. Deliberately small and decoupled from max parallelism
    /// (default 1: one LSM per subtask, the RocksDB shape): a bucket per key group wrote one
    /// file per touched key group per commit — fragmentation proportional to max parallelism.
    /// Key-group locality survives de-bucketing because `kg` leads the primary key, so files'
    /// row groups are kg-clustered and hydration prunes by key-group predicate; rescale pays a
    /// one-time clip at recovery instead of free bucket adoption (see `clip_from_sources`).
    pub buckets: usize,
    /// Paimon `file.format` for state data files.
    pub file_format: String,
    /// Paimon `file.compression` for state data files ("uncompressed", "zstd", "snappy", ...).
    /// Stamped into the table schema, so an external compactor's rewrites honor it too.
    pub file_compression: String,
}

/// A checkpoint's file manifest, handed to the host for upload. `data_files` are immutable,
/// uniquely named, and shared across checkpoints (incremental dedup by name); `meta_files` are the
/// snapshot/manifest/schema documents pinned to this snapshot (re-uploaded each checkpoint —
/// small). All paths are relative to the table root; the host hard-links the files its upload
/// will read into a per-checkpoint directory, so uploads survive local compaction and GC.
#[derive(serde::Serialize)]
pub(crate) struct PaimonCheckpointManifest {
    pub snapshot_id: i64,
    pub data_files: Vec<String>,
    pub meta_files: Vec<String>,
}

/// One working-set entry. `dirty: true` slots are the write buffer — every entry written since
/// the last barrier, pinned until its checkpoint commit. `dirty: false` slots are the current
/// bundle's reads (fetched from the committed table or probed absent) and drop at `end_bundle`.
enum Slot<V> {
    Present { state: V, dirty: bool },
    Absent { dirty: bool },
}

/// The value-agnostic core every Paimon-backed store shares: table lifecycle, snapshot pinning,
/// hydration scans, commits, rescale bucket adoption, and the checkpoint file protocol
/// (listing, hard-links, local GC). The stores compose it with their own working sets and codecs.
pub(crate) struct PaimonTableCore {
    table: Table,
    /// The table pinned at the last committed snapshot; probes read this.
    read_table: Option<Table>,
    read_snapshot: Option<i64>,
    /// The pinned snapshot's scan splits, planned once (a manifest walk) and reused by every
    /// per-batch key probe until the next commit re-pins the table — the snapshot is immutable,
    /// so the split list cannot go stale within a checkpoint interval.
    read_splits: Option<Vec<DataSplit>>,
    fields: Vec<DataField>,
    config: PaimonStoreConfig,
    /// Relative paths reachable from the last committed snapshot — the previous set minus the
    /// current one is exactly what local GC may unlink after a commit.
    live_files: StdHashSet<String>,
}

/// Read-through Paimon-backed store, generic over the operator's value codec (see the module
/// docs).
pub(crate) struct PaimonStore<C: PaimonStateCodec> {
    core: PaimonTableCore,
    codec: C,
    /// The codec's value columns as Arrow fields, in persisted order after `kg`/`k`.
    value_fields: Vec<Field>,
    working: ahash::HashMap<ByteKey, Slot<C::Value>>,
    footprint: isize,
}

impl<C: PaimonStateCodec> KeyedStateStore<C::Value> for PaimonStore<C> {
    #[inline]
    fn contains(&self, key: &[u8]) -> bool {
        matches!(self.working.get(key), Some(Slot::Present { .. }))
    }

    #[inline]
    fn get(&self, key: &[u8]) -> Option<&C::Value> {
        match self.working.get(key) {
            Some(Slot::Present { state, .. }) => Some(state),
            _ => None,
        }
    }

    #[inline]
    fn get_mut(&mut self, key: &[u8]) -> Option<&mut C::Value> {
        match self.working.get_mut(key) {
            Some(Slot::Present { state, dirty }) => {
                *dirty = true;
                Some(state)
            }
            _ => None,
        }
    }

    #[inline]
    fn insert(&mut self, key: ByteKey, value: C::Value) -> &mut C::Value {
        let slot = self
            .working
            .entry(key)
            .insert_entry(Slot::Present { state: value, dirty: true })
            .into_mut();
        match slot {
            Slot::Present { state, .. } => state,
            Slot::Absent { .. } => unreachable!("just inserted a present slot"),
        }
    }

    #[inline]
    fn remove(&mut self, key: &[u8]) {
        if let Some(slot) = self.working.get_mut(key) {
            *slot = Slot::Absent { dirty: true };
        }
    }

    fn begin_batch(
        &mut self,
        batch: &RecordBatch,
        key_columns: &[usize],
        key_timestamp_precisions: &[i32],
    ) -> Result<(), DataFusionError> {
        let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, key_timestamp_precisions);
        let mut misses: Vec<ByteKey> = Vec::new();
        let mut seen: StdHashSet<ByteKey> = StdHashSet::new();
        for row in 0..batch.num_rows() {
            let key = encoder.encode(row);
            if !self.working.contains_key(key) && !seen.contains(key) {
                let owned = ByteKey::from(key);
                seen.insert(owned.clone());
                misses.push(owned);
            }
        }
        if !misses.is_empty() {
            self.fetch_missing(misses)?;
        }
        Ok(())
    }

    fn end_bundle(&mut self) -> Result<(), DataFusionError> {
        // Only the write buffer survives the bundle: clean slots are this bundle's join output
        // and drop here — a later bundle that touches the same keys re-reads them from the
        // page-cached table instead of a second in-memory copy of the state.
        let footprint = &mut self.footprint;
        let codec = &self.codec;
        self.working.retain(|key, slot| match slot {
            Slot::Present { state, dirty: false } => {
                *footprint -= (byte_key_bytes(&key.0)
                    + codec.value_bytes(state)
                    + Self::SLOT_OVERHEAD) as isize;
                false
            }
            Slot::Absent { dirty: false } => {
                *footprint -= Self::SLOT_OVERHEAD as isize;
                false
            }
            _ => true,
        });
        Ok(())
    }

    fn footprint_delta(&mut self) -> isize {
        std::mem::take(&mut self.footprint)
    }
}

impl PaimonTableCore {
    /// Creates a fresh table under `config.table_dir` (schema document + directory skeleton).
    fn create(config: PaimonStoreConfig, schema: PaimonSchema) -> Result<Self, DataFusionError> {
        let table_schema = TableSchema::new(0, &schema);
        let file_io = Self::file_io(&config.table_dir)?;
        runtime().block_on(async {
            file_io
                .mkdirs(&format!("{}/schema", config.table_dir))
                .await
                .map_err(pe)?;
            file_io
                .mkdirs(&format!("{}/snapshot", config.table_dir))
                .await
                .map_err(pe)?;
            let path = format!("{}/schema/schema-{}", config.table_dir, table_schema.id());
            let json = serde_json::to_vec(&table_schema)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            file_io
                .new_output(&path)
                .map_err(pe)?
                .write(bytes::Bytes::from(json))
                .await
                .map_err(pe)
        })?;
        Self::open_at(config, file_io, table_schema, None)
    }

    /// Opens a table directory previously materialized from a checkpoint, pinned at its snapshot.
    fn open(config: PaimonStoreConfig, snapshot_id: i64) -> Result<Self, DataFusionError> {
        let file_io = Self::file_io(&config.table_dir)?;
        let table_schema = Self::latest_schema(&file_io, &config.table_dir)?;
        Self::open_at(config, file_io, table_schema, Some(snapshot_id))
    }

    fn open_at(
        config: PaimonStoreConfig,
        file_io: FileIO,
        table_schema: TableSchema,
        snapshot_id: Option<i64>,
    ) -> Result<Self, DataFusionError> {
        let fields = table_schema.fields().to_vec();
        let table = Table::new(
            file_io,
            Identifier::new("streamfusion", "state"),
            config.table_dir.clone(),
            table_schema,
            None,
        );
        let mut core = PaimonTableCore {
            read_table: None,
            read_snapshot: None,
            read_splits: None,
            fields,
            table,
            config,
            live_files: StdHashSet::new(),
        };
        if let Some(id) = snapshot_id {
            core.read_snapshot = Some(id);
            core.read_table = Some(Self::pin(&core.table, id));
            core.live_files = core.reachable_files(id)?.into_iter().collect();
        }
        Ok(core)
    }

    /// Opens a restored source table pinned at its checkpoint snapshot.
    fn open_source(source_dir: &str, snapshot_id: i64) -> Result<Table, DataFusionError> {
        let file_io = Self::file_io(source_dir)?;
        let schema = Self::latest_schema(&file_io, source_dir)?;
        let source = Table::new(
            file_io,
            Identifier::new("streamfusion", "state"),
            source_dir.to_string(),
            schema,
            None,
        );
        Ok(Self::pin(&source, snapshot_id))
    }

    /// The aligned-restore fast path: the single source covers exactly this subtask's key-group
    /// range, so every bucket is adopted wholesale — data files hard-linked, committed by
    /// existing metadata, no row read or rewritten. Returns `false` without adopting when the
    /// source was written with a different bucket count: its rows sit in buckets this table's
    /// `kg mod buckets` would never look in, so the restore must clip-rewrite instead.
    fn adopt_all(&mut self, source_dir: &str, snapshot_id: i64) -> Result<bool, DataFusionError> {
        let source_file_io = Self::file_io(source_dir)?;
        let source_schema = Self::latest_schema(&source_file_io, source_dir)?;
        let source_buckets = source_schema.options().get("bucket").cloned();
        if source_buckets.as_deref() != Some(&self.config.buckets.to_string()) {
            return Ok(false);
        }
        let pinned = Self::open_source(source_dir, snapshot_id)?;
        let builder = pinned.new_read_builder();
        let plan = runtime()
            .block_on(builder.new_scan().plan())
            .map_err(pe)?;
        let mut messages: Vec<CommitMessage> = Vec::new();
        for split in plan.splits() {
            let bucket = split.bucket();
            let bucket_dir = format!("{}/bucket-{}", self.config.table_dir, bucket);
            std::fs::create_dir_all(&bucket_dir).map_err(io)?;
            for file in split.data_files() {
                let from = format!("{}/bucket-{}/{}", source_dir, bucket, file.file_name);
                let to = format!("{}/{}", bucket_dir, file.file_name);
                if !std::path::Path::new(&to).exists() {
                    std::fs::hard_link(&from, &to).map_err(io)?;
                }
            }
            messages.push(CommitMessage::new(
                EMPTY_SERIALIZED_ROW.to_vec(),
                bucket,
                split.data_files().to_vec(),
            ));
        }
        if !messages.is_empty() {
            let builder = self.table.new_write_builder();
            runtime()
                .block_on(builder.new_commit().commit(messages))
                .map_err(pe)?;
            self.refresh_after_commit()?;
        }
        Ok(true)
    }

    /// The rescale path — RocksDB's restore-time clip, in Paimon terms: buckets are not
    /// partitioned by key group, so a resized subtask reads each source with a key-group range
    /// predicate (`kg` leads the primary key, so row-group pruning keeps the read proportional)
    /// and rewrites the surviving rows into its fresh table in one commit. Sources hold disjoint
    /// key-group ranges, so every rewritten primary key is unique and write order is irrelevant.
    fn clip_from_sources(
        &mut self,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        write_fields: &[Field],
    ) -> Result<(), DataFusionError> {
        let mut clipped: Vec<RecordBatch> = Vec::new();
        for (source_dir, snapshot_id) in sources {
            let pinned = Self::open_source(source_dir, *snapshot_id)?;
            let fields = pinned.schema().fields().to_vec();
            let builder_pred = PredicateBuilder::new(&fields);
            let predicate = Predicate::and(vec![
                builder_pred
                    .greater_or_equal(KG_COLUMN, Datum::Int(*key_groups.start()))
                    .map_err(pe)?,
                builder_pred
                    .less_or_equal(KG_COLUMN, Datum::Int(*key_groups.end()))
                    .map_err(pe)?,
            ]);
            let mut builder = pinned.new_read_builder();
            builder.with_filter(predicate);
            let batches = runtime()
                .block_on(async {
                    let plan = builder.new_scan().plan().await?;
                    let read = builder.new_read()?;
                    let mut stream = read.to_arrow(&plan.splits().to_vec())?;
                    let mut batches = Vec::new();
                    use futures::StreamExt;
                    while let Some(batch) = stream.next().await {
                        batches.push(batch?);
                    }
                    Ok::<_, paimon::Error>(batches)
                })
                .map_err(pe)?;
            for batch in batches {
                // The predicate pushdown is best-effort: re-check the range per row, and
                // normalize reader column types to the write schema.
                let kgs = normalized_column(&batch, 0, &write_fields[0])?;
                let kgs = kgs
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| DataFusionError::Internal("paimon kg column".into()))?;
                let keep: Vec<u32> = (0..batch.num_rows() as u32)
                    .filter(|&row| key_groups.contains(&kgs.value(row as usize)))
                    .collect();
                if keep.is_empty() {
                    continue;
                }
                let indices = arrow::array::UInt32Array::from(keep);
                let mut columns: Vec<ArrayRef> = Vec::with_capacity(write_fields.len() + 1);
                for (i, field) in write_fields.iter().enumerate() {
                    let column = normalized_column(&batch, i, field)?;
                    columns.push(
                        arrow::compute::take(&column, &indices, None)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?,
                    );
                }
                columns.push(Arc::new(Int8Array::from(vec![0i8; indices.len()])));
                let mut fields: Vec<Field> = write_fields.to_vec();
                fields.push(Field::new(VALUE_KIND_COLUMN, DataType::Int8, false));
                clipped.push(
                    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                        .expect("paimon clip batch"),
                );
            }
        }
        if !clipped.is_empty() {
            self.commit_batches(&clipped)?;
        }
        Ok(())
    }

    fn file_io(dir: &str) -> Result<FileIO, DataFusionError> {
        FileIO::from_path(dir)
            .map_err(pe)?
            .with_operator(crate::state::state_fs::state_fs_operator()?)
            .build()
            .map_err(pe)
    }

    fn latest_schema(file_io: &FileIO, dir: &str) -> Result<TableSchema, DataFusionError> {
        runtime().block_on(async {
            let manager = paimon::table::SchemaManager::new(file_io.clone(), dir.to_string());
            let schema = manager
                .latest()
                .await
                .map_err(pe)?
                .ok_or_else(|| DataFusionError::Plan(format!("no paimon schema under {dir}")))?;
            Ok(Arc::unwrap_or_clone(schema))
        })
    }

    fn pin(table: &Table, snapshot_id: i64) -> Table {
        table.copy_with_options(
            [("scan.snapshot-id".to_string(), snapshot_id.to_string())].into(),
        )
    }

    /// The shared leading schema columns of every store: the key-group bucket column and the
    /// BinaryRow key, plus the per-store columns appended by the caller.
    fn schema_builder(
        config: &PaimonStoreConfig,
    ) -> Result<paimon::spec::SchemaBuilder, DataFusionError> {
        Ok(PaimonSchema::builder()
            .column(KG_COLUMN, PaimonType::Int(IntType::new()))
            .column(
                KEY_COLUMN,
                PaimonType::VarBinary(
                    VarBinaryType::try_new(true, VarBinaryType::MAX_LENGTH).map_err(pe)?,
                ),
            )
            .option("bucket", &config.buckets.to_string())
            .option("bucket-key", KG_COLUMN)
            .option("bucket-function.type", "mod")
            .option("file.format", &config.file_format)
            .option("file.compression", &config.file_compression)
            .option("merge-engine", "deduplicate"))
    }

    fn key_group(&self, key: &[u8]) -> i32 {
        flink_key_group(hash_bytes_by_words(key), self.config.max_parallelism) as i32
    }

    /// The pinned snapshot's splits, planned lazily once per pin (see `read_splits`).
    fn pinned_splits(&mut self) -> Result<&[DataSplit], DataFusionError> {
        if self.read_splits.is_none() {
            let read_table = self.read_table.as_ref().expect("pinned read table");
            let builder = read_table.new_read_builder();
            let plan = runtime().block_on(builder.new_scan().plan()).map_err(pe)?;
            self.read_splits = Some(plan.splits().to_vec());
        }
        Ok(self.read_splits.as_deref().expect("planned splits"))
    }

    /// Reads the committed rows for exactly the given missing keys — the disk side of the
    /// per-batch join between an input batch's keys and (write buffer ∪ table). The key set is
    /// pushed into the reader as an `IN` predicate and enforced exactly at parquet decode (stats
    /// prune files and pages; a single hash-set pass filters rows), so returned batches hold only
    /// requested keys and only their value columns decode. A `kg IN` predicate rides along
    /// because the key-group column leads the primary key: files are kg-clustered, so it is the
    /// stats-prunable form of the same key set. Empty when no snapshot is pinned yet.
    fn scan_keys(&mut self, misses: &[ByteKey]) -> Result<Vec<RecordBatch>, DataFusionError> {
        if self.read_table.is_none() || misses.is_empty() {
            return Ok(Vec::new());
        }
        let buckets = self.config.buckets as i32;
        let mut key_groups: Vec<i32> = misses.iter().map(|key| self.key_group(&key.0)).collect();
        key_groups.sort_unstable();
        key_groups.dedup();
        let wanted: StdHashSet<i32> = key_groups.iter().map(|kg| kg % buckets).collect();
        let builder_pred = PredicateBuilder::new(&self.fields);
        let predicate = Predicate::and(vec![
            builder_pred
                .is_in(
                    KG_COLUMN,
                    key_groups.iter().map(|kg| Datum::Int(*kg)).collect(),
                )
                .map_err(pe)?,
            builder_pred
                .is_in(
                    KEY_COLUMN,
                    misses.iter().map(|key| Datum::Bytes(key.0.to_vec())).collect(),
                )
                .map_err(pe)?,
        ]);
        let splits: Vec<DataSplit> = self
            .pinned_splits()?
            .iter()
            .filter(|split| wanted.contains(&split.bucket()))
            .cloned()
            .collect();
        self.read_splits_with_filter(&splits, predicate)
    }

    /// Reads the committed rows matching an arbitrary predicate across all buckets — the disk
    /// side of a range read (watermark firing). Same split reuse as `scan_keys`; callers re-check
    /// rows where correctness demands it, since predicate pushdown is exact only for supported
    /// shapes. Empty when no snapshot is pinned yet.
    fn scan_predicate(&mut self, predicate: Predicate) -> Result<Vec<RecordBatch>, DataFusionError> {
        if self.read_table.is_none() {
            return Ok(Vec::new());
        }
        let splits = self.pinned_splits()?.to_vec();
        self.read_splits_with_filter(&splits, predicate)
    }

    fn read_splits_with_filter(
        &self,
        splits: &[DataSplit],
        predicate: Predicate,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let read_table = self.read_table.as_ref().expect("pinned read table");
        let mut builder = read_table.new_read_builder();
        builder.with_filter(predicate);
        runtime()
            .block_on(async {
                let read = builder.new_read()?;
                let mut stream = read.to_arrow(splits)?;
                let mut batches = Vec::new();
                use futures::StreamExt;
                while let Some(batch) = stream.next().await {
                    batches.push(batch?);
                }
                Ok::<_, paimon::Error>(batches)
            })
            .map_err(pe)
    }

    /// Commits one write batch as a new snapshot and re-pins reads on it.
    fn commit(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        self.commit_batches(std::slice::from_ref(batch))
    }

    /// Commits a sequence of write batches, in order, as ONE new snapshot and re-pins reads.
    fn commit_batches(&mut self, batches: &[RecordBatch]) -> Result<(), DataFusionError> {
        let builder = self.table.new_write_builder();
        runtime()
            .block_on(async {
                let mut write = builder.new_write()?;
                for batch in batches {
                    write.write_arrow_batch(batch).await?;
                }
                let messages = write.prepare_commit().await?;
                builder.new_commit().commit(messages).await
            })
            .map_err(pe)?;
        self.refresh_after_commit()
    }

    /// The checkpoint file phase, after the dirty commit: garbage-collect local files no longer
    /// reachable and return the manifest for upload. Hard-linking the files an upload will read
    /// happens host-side, which knows which files are new against the last confirmed checkpoint —
    /// linking every reachable file here re-linked the whole table each barrier.
    fn checkpoint_manifest(&mut self) -> Result<PaimonCheckpointManifest, DataFusionError> {
        let Some(snapshot_id) = self.read_snapshot else {
            return Ok(PaimonCheckpointManifest {
                snapshot_id: -1,
                data_files: Vec::new(),
                meta_files: Vec::new(),
            });
        };
        let (data_files, meta_files) = self.snapshot_file_listing(snapshot_id)?;
        self.gc_local(&data_files, &meta_files)?;
        Ok(PaimonCheckpointManifest { snapshot_id, data_files, meta_files })
    }

    fn refresh_after_commit(&mut self) -> Result<(), DataFusionError> {
        self.refresh_to_latest()?;
        if self.read_snapshot.is_none() {
            return Err(DataFusionError::Internal("commit produced no snapshot".into()));
        }
        Ok(())
    }

    /// Re-pins reads at the table's latest committed snapshot, if it moved.
    fn refresh_to_latest(&mut self) -> Result<(), DataFusionError> {
        let latest = runtime()
            .block_on(self.table.snapshot_manager().get_latest_snapshot_id())
            .map_err(pe)?;
        if let Some(latest) = latest {
            if self.read_snapshot != Some(latest) {
                self.read_snapshot = Some(latest);
                self.read_table = Some(Self::pin(&self.table, latest));
                self.read_splits = None;
            }
        }
        Ok(())
    }

    /// The relative paths of everything the given snapshot needs: live data files (shared upload
    /// candidates) and the snapshot/manifest/schema documents (private).
    fn snapshot_file_listing(
        &self,
        snapshot_id: i64,
    ) -> Result<(Vec<String>, Vec<String>), DataFusionError> {
        let data_files = self.reachable_data_files(snapshot_id)?;
        let mut meta_files = vec![format!("snapshot/snapshot-{snapshot_id}")];
        let manager = self.table.snapshot_manager();
        let file_io = self.table.file_io().clone();
        let manifest_lists = runtime()
            .block_on(async {
                let snapshot = manager.get_snapshot(snapshot_id).await?;
                let mut lists = vec![
                    snapshot.base_manifest_list().to_string(),
                    snapshot.delta_manifest_list().to_string(),
                ];
                if let Some(index) = snapshot.index_manifest() {
                    lists.push(index.to_string());
                }
                let mut manifests = Vec::new();
                for list in &lists {
                    if list.is_empty() {
                        continue;
                    }
                    for meta in
                        paimon::spec::ManifestList::read(&file_io, &manager.manifest_path(list))
                            .await?
                    {
                        manifests.push(meta.file_name().to_string());
                    }
                }
                lists.extend(manifests);
                Ok::<_, paimon::Error>(lists)
            })
            .map_err(pe)?;
        for name in manifest_lists {
            if !name.is_empty() {
                meta_files.push(format!("manifest/{name}"));
            }
        }
        for entry in std::fs::read_dir(format!("{}/schema", self.config.table_dir)).map_err(io)? {
            let entry = entry.map_err(io)?;
            meta_files.push(format!("schema/{}", entry.file_name().to_string_lossy()));
        }
        Ok((data_files, meta_files))
    }

    fn reachable_data_files(&self, snapshot_id: i64) -> Result<Vec<String>, DataFusionError> {
        let pinned = Self::pin(&self.table, snapshot_id);
        let builder = pinned.new_read_builder();
        let plan = runtime().block_on(builder.new_scan().plan()).map_err(pe)?;
        let mut files = Vec::new();
        for split in plan.splits() {
            for file in split.data_files() {
                files.push(format!("bucket-{}/{}", split.bucket(), file.file_name));
            }
        }
        Ok(files)
    }

    fn reachable_files(&self, snapshot_id: i64) -> Result<Vec<String>, DataFusionError> {
        let (mut data, meta) = self.snapshot_file_listing(snapshot_id)?;
        data.extend(meta);
        Ok(data)
    }

    /// Unlinks local files that the previous snapshot needed and the current one no longer does
    /// (files superseded by compaction, expired snapshot/manifest documents). Uploads for older,
    /// still-pending checkpoints read from their own hard-link dirs, so this is safe immediately.
    fn gc_local(&mut self, data_files: &[String], meta_files: &[String]) -> Result<(), DataFusionError> {
        let next: StdHashSet<String> = data_files.iter().chain(meta_files).cloned().collect();
        for stale in self.live_files.difference(&next) {
            let path = format!("{}/{}", self.config.table_dir, stale);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io(e)),
            }
        }
        self.live_files = next;
        Ok(())
    }
}

impl<C: PaimonStateCodec> PaimonStore<C> {
    const SLOT_OVERHEAD: usize = std::mem::size_of::<Slot<C::Value>>() + GROUP_ENTRY_OVERHEAD;

    /// Creates a fresh table under `config.table_dir` (schema document + directory skeleton).
    pub(crate) fn create(config: PaimonStoreConfig, codec: C) -> Result<Self, DataFusionError> {
        let schema = Self::paimon_schema(&config, &codec)?;
        Self::assemble(PaimonTableCore::create(config, schema)?, codec)
    }

    /// Opens a table directory previously materialized from a checkpoint, pinned at its snapshot.
    pub(crate) fn open(
        config: PaimonStoreConfig,
        codec: C,
        snapshot_id: i64,
    ) -> Result<Self, DataFusionError> {
        Self::assemble(PaimonTableCore::open(config, snapshot_id)?, codec)
    }

    /// Builds a fresh table at `config.table_dir` from one or more restored table directories
    /// (rescale); see `PaimonTableCore::adopt_buckets`.
    pub(crate) fn open_merged(
        config: PaimonStoreConfig,
        codec: C,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        let mut store = Self::create(config, codec)?;
        if aligned && sources.len() == 1 {
            let (source_dir, snapshot_id) = &sources[0];
            if store.core.adopt_all(source_dir, *snapshot_id)? {
                return Ok(store);
            }
        }
        let write_fields = store.arrow_fields();
        store.core.clip_from_sources(sources, key_groups, &write_fields)?;
        Ok(store)
    }

    fn assemble(core: PaimonTableCore, codec: C) -> Result<Self, DataFusionError> {
        if !codec.supported() {
            return Err(DataFusionError::Plan(
                "state shape not supported by the paimon state backend".into(),
            ));
        }
        let value_fields: Vec<Field> = codec
            .value_fields()
            .into_iter()
            .map(|(name, data_type)| Field::new(name, data_type, true))
            .collect();
        Ok(PaimonStore {
            core,
            codec,
            value_fields,
            working: ahash::HashMap::default(),
            footprint: 0,
        })
    }

    fn paimon_schema(
        config: &PaimonStoreConfig,
        codec: &C,
    ) -> Result<PaimonSchema, DataFusionError> {
        let mut builder = PaimonTableCore::schema_builder(config)?;
        for (name, data_type) in codec.value_fields() {
            let paimon_type = paimon_type_of(&data_type).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "state type {data_type} not supported by the paimon state backend"
                ))
            })?;
            builder = builder.column(name, paimon_type);
        }
        builder.primary_key([KG_COLUMN, KEY_COLUMN]).build().map_err(pe)
    }

    /// The Arrow schema of persisted rows (also the write-batch schema, which additionally
    /// carries `_VALUE_KIND`).
    fn arrow_fields(&self) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KG_COLUMN, DataType::Int32, false),
            Field::new(KEY_COLUMN, DataType::Binary, false),
        ];
        fields.extend(self.value_fields.iter().cloned());
        fields
    }

    /// Reads the missed keys from the committed table and records every missed key's result —
    /// present or absent — in the working set for the current bundle.
    fn fetch_missing(&mut self, misses: Vec<ByteKey>) -> Result<(), DataFusionError> {
        for batch in self.core.scan_keys(&misses)? {
            self.absorb_scan_batch(&batch)?;
        }
        let mut added_bytes = 0usize;
        for key in misses {
            self.working.entry(key).or_insert_with(|| {
                // Slot overhead only: if the operator creates this key, its own tracking charges
                // the key and state bytes (see `end_bundle` for the split).
                added_bytes += Self::SLOT_OVERHEAD;
                Slot::Absent { dirty: false }
            });
        }
        self.footprint += added_bytes as isize;
        Ok(())
    }

    /// Decodes scanned rows into clean working-set entries; a key already in the working set
    /// stays authoritative over the table.
    fn absorb_scan_batch(&mut self, batch: &RecordBatch) -> Result<usize, DataFusionError> {
        let expected = self.arrow_fields();
        let key_index = 1;
        let keys = normalized_column(batch, key_index, &expected[key_index])?;
        let keys = keys
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| DataFusionError::Internal("paimon key column".into()))?;
        let mut value_columns: Vec<ArrayRef> = Vec::with_capacity(self.value_fields.len());
        for i in 0..self.value_fields.len() {
            value_columns.push(normalized_column(batch, 2 + i, &expected[2 + i])?);
        }
        let mut added = 0usize;
        let mut added_bytes = 0usize;
        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            if self.working.contains_key(key) {
                continue;
            }
            let mut scalars: Vec<ScalarValue> = Vec::with_capacity(value_columns.len());
            for column in &value_columns {
                scalars.push(
                    ScalarValue::try_from_array(column, row)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                );
            }
            let state = self.codec.decode(&scalars);
            let owned = ByteKey::from(key);
            added_bytes +=
                byte_key_bytes(&owned.0) + self.codec.value_bytes(&state) + Self::SLOT_OVERHEAD;
            self.working
                .insert(owned, Slot::Present { state, dirty: false });
            added += 1;
        }
        self.footprint += added_bytes as isize;
        Ok(added)
    }

    /// Builds the write batch for all dirty slots: upserts carry the encoded state row, deletions
    /// a `_VALUE_KIND = 3` tombstone. Returns `None` when nothing changed since the last commit.
    fn dirty_batch(&self) -> Option<RecordBatch> {
        let num_value = self.value_fields.len();
        let mut kgs: Vec<i32> = Vec::new();
        let mut keys: Vec<&[u8]> = Vec::new();
        let mut values: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_value];
        let mut kinds: Vec<i8> = Vec::new();
        for (key, slot) in self.working.iter() {
            match slot {
                Slot::Present { state, dirty: true } => {
                    kgs.push(self.core.key_group(&key.0));
                    keys.push(&key.0);
                    for (i, scalar) in self.codec.encode(state).into_iter().enumerate() {
                        values[i].push(scalar);
                    }
                    kinds.push(0); // +I upsert — deduplicate keeps the latest by sequence
                }
                Slot::Absent { dirty: true } => {
                    kgs.push(self.core.key_group(&key.0));
                    keys.push(&key.0);
                    for (i, field) in self.value_fields.iter().enumerate() {
                        values[i].push(null_scalar(field.data_type()));
                    }
                    kinds.push(3); // -D tombstone
                }
                _ => {}
            }
        }
        if keys.is_empty() {
            return None;
        }
        let mut fields = self.arrow_fields();
        fields.push(Field::new(VALUE_KIND_COLUMN, DataType::Int8, false));
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(kgs)),
            Arc::new(BinaryArray::from_iter_values(keys)),
        ];
        for (i, field) in self.value_fields.iter().enumerate() {
            columns.push(scalars_to_array(std::mem::take(&mut values[i]), field.data_type()));
        }
        columns.push(Arc::new(Int8Array::from(kinds)));
        Some(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("paimon dirty write batch"),
        )
    }

    /// Checkpoint sync phase, called at the barrier: commit the dirty write buffer as the
    /// checkpoint's snapshot and run the checkpoint file phase (see
    /// `PaimonTableCore::checkpoint_manifest`).
    pub(crate) fn checkpoint(&mut self) -> Result<PaimonCheckpointManifest, DataFusionError> {
        // An external compactor (the Java Paimon glue) may have committed a maintenance snapshot
        // just before this call: adopt the latest snapshot so the flush lands on top of it, the
        // manifest lists it, and local GC sees its file set.
        self.core.refresh_to_latest()?;
        if let Some(batch) = self.dirty_batch() {
            self.core.commit(&batch)?;
        }
        // All dirty slots are durable now; drop them (pure read-through, no cache across bundles).
        let footprint = &mut self.footprint;
        let codec = &self.codec;
        self.working.retain(|key, slot| {
            match slot {
                Slot::Present { state, .. } => {
                    *footprint -= (byte_key_bytes(&key.0)
                        + codec.value_bytes(state)
                        + Self::SLOT_OVERHEAD) as isize;
                }
                Slot::Absent { .. } => *footprint -= Self::SLOT_OVERHEAD as isize,
            }
            false
        });
        self.core.checkpoint_manifest()
    }
}

const ORD_COLUMN: &str = "ord";

/// The per-operator half of a LIST-state store (the analog of Flink's `ListState`): a key's value
/// is an ordered collection persisted as one table row per element under PK `[kg, k, ord]`, where
/// `ord` is the element's position. A dirty key rewrites its whole list (upserts `0..len`,
/// tombstones `len..persisted_len`) — exactly the whole-value rewrite Flink's RocksDB `ListState`
/// pays on every mutation — and hydration reassembles the exact order by `ord`, so
/// position-sensitive semantics (Top-N tie order) survive restore byte-for-byte.
pub(crate) trait PaimonListCodec {
    type Entry;

    /// Whether this operator instance's element shape is persistable at all.
    fn supported(&self) -> bool;

    /// The element columns beyond `kg`/`k`/`ord`, in persisted order. All stored nullable.
    fn value_fields(&self) -> Vec<(String, DataType)>;

    /// Encodes one element as one scalar per element column.
    fn encode(&self, entry: &Self::Entry) -> Vec<ScalarValue>;

    /// Decodes one probe row — the inverse of `encode`.
    fn decode(&self, scalars: &[ScalarValue]) -> Self::Entry;

    /// One element's accounted heap footprint.
    fn entry_bytes(&self, entry: &Self::Entry) -> usize;
}

enum ListSlot<E> {
    Present { entries: Vec<E>, dirty: bool, persisted_len: usize },
    Absent { dirty: bool, persisted_len: usize },
}

impl<E> ListSlot<E> {
    fn persisted_len(&self) -> usize {
        match self {
            ListSlot::Present { persisted_len, .. } | ListSlot::Absent { persisted_len, .. } => {
                *persisted_len
            }
        }
    }
}

/// Read-through Paimon-backed list store (see `PaimonListCodec`); same working-set and checkpoint
/// discipline as the single-value store, over the shared table core.
pub(crate) struct PaimonListStore<C: PaimonListCodec> {
    core: PaimonTableCore,
    codec: C,
    /// The codec's element columns as Arrow fields, in persisted order after `kg`/`k`/`ord`.
    value_fields: Vec<Field>,
    working: ahash::HashMap<ByteKey, ListSlot<C::Entry>>,
    footprint: isize,
}

impl<C: PaimonListCodec> KeyedStateStore<Vec<C::Entry>> for PaimonListStore<C> {
    #[inline]
    fn contains(&self, key: &[u8]) -> bool {
        matches!(self.working.get(key), Some(ListSlot::Present { .. }))
    }

    #[inline]
    fn get(&self, key: &[u8]) -> Option<&Vec<C::Entry>> {
        match self.working.get(key) {
            Some(ListSlot::Present { entries, .. }) => Some(entries),
            _ => None,
        }
    }

    #[inline]
    fn get_mut(&mut self, key: &[u8]) -> Option<&mut Vec<C::Entry>> {
        match self.working.get_mut(key) {
            Some(ListSlot::Present { entries, dirty, .. }) => {
                *dirty = true;
                Some(entries)
            }
            _ => None,
        }
    }

    #[inline]
    fn insert(&mut self, key: ByteKey, value: Vec<C::Entry>) -> &mut Vec<C::Entry> {
        // An overwritten slot keeps its persisted length: the elements already in the table still
        // need tombstones beyond the new list's length at the next checkpoint.
        let persisted_len = self.working.get(&*key.0).map_or(0, ListSlot::persisted_len);
        let slot = self
            .working
            .entry(key)
            .insert_entry(ListSlot::Present { entries: value, dirty: true, persisted_len })
            .into_mut();
        match slot {
            ListSlot::Present { entries, .. } => entries,
            ListSlot::Absent { .. } => unreachable!("just inserted a present slot"),
        }
    }

    #[inline]
    fn remove(&mut self, key: &[u8]) {
        if let Some(slot) = self.working.get_mut(key) {
            let persisted_len = slot.persisted_len();
            *slot = ListSlot::Absent { dirty: true, persisted_len };
        }
    }

    fn begin_batch(
        &mut self,
        batch: &RecordBatch,
        key_columns: &[usize],
        key_timestamp_precisions: &[i32],
    ) -> Result<(), DataFusionError> {
        let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, key_timestamp_precisions);
        let mut misses: Vec<ByteKey> = Vec::new();
        let mut seen: StdHashSet<ByteKey> = StdHashSet::new();
        for row in 0..batch.num_rows() {
            let key = encoder.encode(row);
            if !self.working.contains_key(key) && !seen.contains(key) {
                let owned = ByteKey::from(key);
                seen.insert(owned.clone());
                misses.push(owned);
            }
        }
        if !misses.is_empty() {
            self.fetch_missing(misses)?;
        }
        Ok(())
    }

    fn end_bundle(&mut self) -> Result<(), DataFusionError> {
        // See the single-value store: only the write buffer survives the bundle.
        let footprint = &mut self.footprint;
        let codec = &self.codec;
        self.working.retain(|key, slot| match slot {
            ListSlot::Present { entries, dirty: false, .. } => {
                *footprint -= (byte_key_bytes(&key.0)
                    + entries.iter().map(|e| codec.entry_bytes(e)).sum::<usize>()
                    + Self::SLOT_OVERHEAD) as isize;
                false
            }
            ListSlot::Absent { dirty: false, .. } => {
                *footprint -= Self::SLOT_OVERHEAD as isize;
                false
            }
            _ => true,
        });
        Ok(())
    }

    fn footprint_delta(&mut self) -> isize {
        std::mem::take(&mut self.footprint)
    }
}

impl<C: PaimonListCodec> PaimonListStore<C> {
    const SLOT_OVERHEAD: usize =
        std::mem::size_of::<ListSlot<C::Entry>>() + GROUP_ENTRY_OVERHEAD;

    /// Creates a fresh table under `config.table_dir` (schema document + directory skeleton).
    pub(crate) fn create(config: PaimonStoreConfig, codec: C) -> Result<Self, DataFusionError> {
        let schema = Self::paimon_schema(&config, &codec)?;
        Self::assemble(PaimonTableCore::create(config, schema)?, codec)
    }

    /// Opens a table directory previously materialized from a checkpoint, pinned at its snapshot.
    pub(crate) fn open(
        config: PaimonStoreConfig,
        codec: C,
        snapshot_id: i64,
    ) -> Result<Self, DataFusionError> {
        Self::assemble(PaimonTableCore::open(config, snapshot_id)?, codec)
    }

    /// Builds a fresh table at `config.table_dir` from one or more restored table directories
    /// (rescale); see `PaimonTableCore::adopt_buckets`.
    pub(crate) fn open_merged(
        config: PaimonStoreConfig,
        codec: C,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        let mut store = Self::create(config, codec)?;
        if aligned && sources.len() == 1 {
            let (source_dir, snapshot_id) = &sources[0];
            if store.core.adopt_all(source_dir, *snapshot_id)? {
                return Ok(store);
            }
        }
        let write_fields = store.arrow_fields();
        store.core.clip_from_sources(sources, key_groups, &write_fields)?;
        Ok(store)
    }

    fn assemble(core: PaimonTableCore, codec: C) -> Result<Self, DataFusionError> {
        if !codec.supported() {
            return Err(DataFusionError::Plan(
                "state shape not supported by the paimon state backend".into(),
            ));
        }
        let value_fields: Vec<Field> = codec
            .value_fields()
            .into_iter()
            .map(|(name, data_type)| Field::new(name, data_type, true))
            .collect();
        Ok(PaimonListStore {
            core,
            codec,
            value_fields,
            working: ahash::HashMap::default(),
            footprint: 0,
        })
    }

    fn paimon_schema(
        config: &PaimonStoreConfig,
        codec: &C,
    ) -> Result<PaimonSchema, DataFusionError> {
        let mut builder = PaimonTableCore::schema_builder(config)?
            .column(ORD_COLUMN, PaimonType::BigInt(BigIntType::new()));
        for (name, data_type) in codec.value_fields() {
            let paimon_type = paimon_type_of(&data_type).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "state type {data_type} not supported by the paimon state backend"
                ))
            })?;
            builder = builder.column(name, paimon_type);
        }
        builder
            .primary_key([KG_COLUMN, KEY_COLUMN, ORD_COLUMN])
            .build()
            .map_err(pe)
    }

    /// The Arrow schema of persisted rows (also the write-batch schema, which additionally
    /// carries `_VALUE_KIND`).
    fn arrow_fields(&self) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KG_COLUMN, DataType::Int32, false),
            Field::new(KEY_COLUMN, DataType::Binary, false),
            Field::new(ORD_COLUMN, DataType::Int64, false),
        ];
        fields.extend(self.value_fields.iter().cloned());
        fields
    }

    /// Reads the missed keys from the committed table. Elements are collected across ALL probe
    /// batches before assembly — the merge reader may split one key's rows across batch
    /// boundaries — then reassembled in `ord` order.
    fn fetch_missing(&mut self, misses: Vec<ByteKey>) -> Result<(), DataFusionError> {
        let batches = self.core.scan_keys(&misses)?;
        let mut collected: ahash::HashMap<ByteKey, Vec<(i64, Vec<ScalarValue>)>> =
            ahash::HashMap::default();
        for batch in &batches {
            let expected = self.arrow_fields();
            let keys = normalized_column(batch, 1, &expected[1])?;
            let keys = keys
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| DataFusionError::Internal("paimon key column".into()))?;
            let ords = normalized_column(batch, 2, &expected[2])?;
            let ords = ords
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("paimon ord column".into()))?;
            let mut value_columns: Vec<ArrayRef> = Vec::with_capacity(self.value_fields.len());
            for i in 0..self.value_fields.len() {
                value_columns.push(normalized_column(batch, 3 + i, &expected[3 + i])?);
            }
            for row in 0..batch.num_rows() {
                let key = keys.value(row);
                // A key already in the working set stays authoritative over the table.
                if self.working.contains_key(key) {
                    continue;
                }
                let mut scalars: Vec<ScalarValue> = Vec::with_capacity(value_columns.len());
                for column in &value_columns {
                    scalars.push(
                        ScalarValue::try_from_array(column, row)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?,
                    );
                }
                collected
                    .entry(ByteKey::from(key))
                    .or_default()
                    .push((ords.value(row), scalars));
            }
        }
        let mut added_bytes = 0usize;
        for (key, mut rows) in collected {
            rows.sort_by_key(|(ord, _)| *ord);
            let persisted_len = rows.last().map_or(0, |(ord, _)| *ord as usize + 1);
            let entries: Vec<C::Entry> =
                rows.iter().map(|(_, scalars)| self.codec.decode(scalars)).collect();
            added_bytes += byte_key_bytes(&key.0)
                + entries.iter().map(|e| self.codec.entry_bytes(e)).sum::<usize>()
                + Self::SLOT_OVERHEAD;
            self.working.insert(
                key,
                ListSlot::Present { entries, dirty: false, persisted_len },
            );
        }
        for key in misses {
            self.working.entry(key).or_insert_with(|| {
                added_bytes += Self::SLOT_OVERHEAD;
                ListSlot::Absent { dirty: false, persisted_len: 0 }
            });
        }
        self.footprint += added_bytes as isize;
        Ok(())
    }

    /// Builds the write batch for all dirty slots: element upserts at `ord = 0..len`, tombstones
    /// for every persisted position at or beyond the new length. At most one row per `(k, ord)`
    /// per checkpoint by construction — required, since within one commit equal-PK rows resolve by
    /// arrival order, which iteration here does not define.
    fn dirty_batch(&self) -> Option<RecordBatch> {
        let num_value = self.value_fields.len();
        let mut kgs: Vec<i32> = Vec::new();
        let mut keys: Vec<&[u8]> = Vec::new();
        let mut ords: Vec<i64> = Vec::new();
        let mut values: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_value];
        let mut kinds: Vec<i8> = Vec::new();
        for (key, slot) in self.working.iter() {
            let (entries, persisted_len): (&[C::Entry], usize) = match slot {
                ListSlot::Present { entries, dirty: true, persisted_len } => {
                    (entries, *persisted_len)
                }
                ListSlot::Absent { dirty: true, persisted_len } => (&[], *persisted_len),
                _ => continue,
            };
            let kg = self.core.key_group(&key.0);
            for (i, entry) in entries.iter().enumerate() {
                kgs.push(kg);
                keys.push(&key.0);
                ords.push(i as i64);
                for (column, scalar) in values.iter_mut().zip(self.codec.encode(entry)) {
                    column.push(scalar);
                }
                kinds.push(0); // +I upsert — deduplicate keeps the latest by sequence
            }
            for i in entries.len()..persisted_len {
                kgs.push(kg);
                keys.push(&key.0);
                ords.push(i as i64);
                for (column, field) in values.iter_mut().zip(self.value_fields.iter()) {
                    column.push(null_scalar(field.data_type()));
                }
                kinds.push(3); // -D tombstone for a vacated position
            }
        }
        if keys.is_empty() {
            return None;
        }
        let mut fields = self.arrow_fields();
        fields.push(Field::new(VALUE_KIND_COLUMN, DataType::Int8, false));
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(kgs)),
            Arc::new(BinaryArray::from_iter_values(keys)),
            Arc::new(Int64Array::from(ords)),
        ];
        for (i, field) in self.value_fields.iter().enumerate() {
            columns.push(scalars_to_array(std::mem::take(&mut values[i]), field.data_type()));
        }
        columns.push(Arc::new(Int8Array::from(kinds)));
        Some(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("paimon list dirty write batch"),
        )
    }

    /// Checkpoint sync phase, called at the barrier; see the single-value store's `checkpoint`.
    pub(crate) fn checkpoint(&mut self) -> Result<PaimonCheckpointManifest, DataFusionError> {
        self.core.refresh_to_latest()?;
        if let Some(batch) = self.dirty_batch() {
            self.core.commit(&batch)?;
        }
        let footprint = &mut self.footprint;
        let codec = &self.codec;
        self.working.retain(|key, slot| {
            match slot {
                ListSlot::Present { entries, .. } => {
                    *footprint -= (byte_key_bytes(&key.0)
                        + entries.iter().map(|e| codec.entry_bytes(e)).sum::<usize>()
                        + Self::SLOT_OVERHEAD) as isize;
                }
                ListSlot::Absent { .. } => *footprint -= Self::SLOT_OVERHEAD as isize,
            }
            false
        });
        self.core.checkpoint_manifest()
    }
}

const SUB_KEY_COLUMN: &str = "r";

/// The per-operator half of a MAP-state store (the analog of Flink's `MapState`, and of its join
/// state views): a key's value is a map of rows to per-row metadata, persisted as one table row
/// per entry under PK `[kg, k, r]`, where `r` is a STABLE byte identity of the row — its Flink
/// BinaryRow encoding, not the transient arrow-row bytes the working set keys by. The flush is
/// per-entry: a dirty key writes only the entries that differ from the hydrated image and
/// tombstones the hydrated entries that are gone, so a hot bucket's unchanged rows cost nothing
/// at the barrier.
pub(crate) trait PaimonMapCodec {
    type Entry: Clone + PartialEq;

    /// Whether this operator instance's row shape is persistable at all.
    fn supported(&self) -> bool;

    /// The entry columns beyond `kg`/`k`/`r`, in persisted order. All stored nullable.
    fn value_fields(&self) -> Vec<(String, DataType)>;

    /// The stable sub-key bytes for a stored row (given by its working-set byte encoding).
    fn sub_key(&self, row: &[u8]) -> Vec<u8>;

    /// Encodes one (row, entry) as one scalar per entry column.
    fn encode(&self, row: &[u8], entry: &Self::Entry) -> Vec<ScalarValue>;

    /// Decodes one probe row into the working-set row bytes and its entry.
    fn decode(&self, scalars: &[ScalarValue]) -> (ByteKey, Self::Entry);

    /// One entry's accounted heap footprint.
    fn entry_bytes(&self, row: &[u8], entry: &Self::Entry) -> usize;
}

enum MapSlot<E> {
    Present {
        entries: ahash::HashMap<ByteKey, E>,
        dirty: bool,
        /// The (row → entry) image hydrated from the table — the flush base: a live entry is
        /// written only if it differs from this image, and persisted rows no longer live at the
        /// barrier (`persisted − entries.keys()`) get tombstones.
        persisted: ahash::HashMap<ByteKey, E>,
    },
    Absent {
        dirty: bool,
        persisted: ahash::HashMap<ByteKey, E>,
    },
}

impl<E> MapSlot<E> {
    fn take_persisted(&mut self) -> ahash::HashMap<ByteKey, E> {
        match self {
            MapSlot::Present { persisted, .. } | MapSlot::Absent { persisted, .. } => {
                std::mem::take(persisted)
            }
        }
    }
}

/// Read-through Paimon-backed map store (see `PaimonMapCodec`); same working-set and checkpoint
/// discipline as the other stores, over the shared table core.
pub(crate) struct PaimonMapStore<C: PaimonMapCodec> {
    core: PaimonTableCore,
    codec: C,
    /// The codec's entry columns as Arrow fields, in persisted order after `kg`/`k`/`r`.
    value_fields: Vec<Field>,
    working: ahash::HashMap<ByteKey, MapSlot<C::Entry>>,
    footprint: isize,
}

impl<C: PaimonMapCodec> KeyedStateStore<ahash::HashMap<ByteKey, C::Entry>> for PaimonMapStore<C> {
    #[inline]
    fn contains(&self, key: &[u8]) -> bool {
        matches!(self.working.get(key), Some(MapSlot::Present { .. }))
    }

    #[inline]
    fn get(&self, key: &[u8]) -> Option<&ahash::HashMap<ByteKey, C::Entry>> {
        match self.working.get(key) {
            Some(MapSlot::Present { entries, .. }) => Some(entries),
            _ => None,
        }
    }

    #[inline]
    fn get_mut(&mut self, key: &[u8]) -> Option<&mut ahash::HashMap<ByteKey, C::Entry>> {
        match self.working.get_mut(key) {
            Some(MapSlot::Present { entries, dirty, .. }) => {
                *dirty = true;
                Some(entries)
            }
            _ => None,
        }
    }

    #[inline]
    fn insert(
        &mut self,
        key: ByteKey,
        value: ahash::HashMap<ByteKey, C::Entry>,
    ) -> &mut ahash::HashMap<ByteKey, C::Entry> {
        // An overwritten slot keeps its persisted image: entries already in the table still
        // need tombstones at the next checkpoint if the new map lacks them.
        let persisted = match self.working.get_mut(&*key.0) {
            Some(slot) => slot.take_persisted(),
            None => ahash::HashMap::default(),
        };
        let slot = self
            .working
            .entry(key)
            .insert_entry(MapSlot::Present { entries: value, dirty: true, persisted })
            .into_mut();
        match slot {
            MapSlot::Present { entries, .. } => entries,
            MapSlot::Absent { .. } => unreachable!("just inserted a present slot"),
        }
    }

    #[inline]
    fn remove(&mut self, key: &[u8]) {
        if let Some(slot) = self.working.get_mut(key) {
            let persisted = slot.take_persisted();
            *slot = MapSlot::Absent { dirty: true, persisted };
        }
    }

    fn begin_batch(
        &mut self,
        batch: &RecordBatch,
        key_columns: &[usize],
        key_timestamp_precisions: &[i32],
    ) -> Result<(), DataFusionError> {
        let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, key_timestamp_precisions);
        let mut misses: Vec<ByteKey> = Vec::new();
        let mut seen: StdHashSet<ByteKey> = StdHashSet::new();
        for row in 0..batch.num_rows() {
            let key = encoder.encode(row);
            if !self.working.contains_key(key) && !seen.contains(key) {
                let owned = ByteKey::from(key);
                seen.insert(owned.clone());
                misses.push(owned);
            }
        }
        if !misses.is_empty() {
            self.fetch_missing(misses)?;
        }
        Ok(())
    }

    fn end_bundle(&mut self) -> Result<(), DataFusionError> {
        // See the single-value store: only the write buffer survives the bundle. A dirty slot
        // keeps its persisted image too — it is the flush base the barrier diffs against.
        let footprint = &mut self.footprint;
        let codec = &self.codec;
        self.working.retain(|key, slot| match slot {
            MapSlot::Present { entries, persisted, dirty: false } => {
                *footprint -= (byte_key_bytes(&key.0)
                    + entries.iter().map(|(r, e)| codec.entry_bytes(&r.0, e)).sum::<usize>()
                    + persisted.len() * Self::IMAGE_ENTRY_BYTES
                    + Self::SLOT_OVERHEAD) as isize;
                false
            }
            MapSlot::Absent { persisted, dirty: false } => {
                *footprint -=
                    (persisted.len() * Self::IMAGE_ENTRY_BYTES + Self::SLOT_OVERHEAD) as isize;
                false
            }
            _ => true,
        });
        Ok(())
    }

    fn footprint_delta(&mut self) -> isize {
        std::mem::take(&mut self.footprint)
    }
}

impl<C: PaimonMapCodec> PaimonMapStore<C> {
    const SLOT_OVERHEAD: usize =
        std::mem::size_of::<MapSlot<C::Entry>>() + GROUP_ENTRY_OVERHEAD;
    /// One persisted-image entry: an `Arc` key clone plus the entry copy (the row bytes are
    /// shared with the live map, so they are accounted once, by `entry_bytes`).
    const IMAGE_ENTRY_BYTES: usize =
        std::mem::size_of::<(ByteKey, C::Entry)>() + GROUP_ENTRY_OVERHEAD;

    /// Creates a fresh table under `config.table_dir` (schema document + directory skeleton).
    pub(crate) fn create(config: PaimonStoreConfig, codec: C) -> Result<Self, DataFusionError> {
        let schema = Self::paimon_schema(&config, &codec)?;
        Self::assemble(PaimonTableCore::create(config, schema)?, codec)
    }

    /// Opens a table directory previously materialized from a checkpoint, pinned at its snapshot.
    pub(crate) fn open(
        config: PaimonStoreConfig,
        codec: C,
        snapshot_id: i64,
    ) -> Result<Self, DataFusionError> {
        Self::assemble(PaimonTableCore::open(config, snapshot_id)?, codec)
    }

    /// Builds a fresh table at `config.table_dir` from one or more restored table directories
    /// (rescale); see `PaimonTableCore::adopt_buckets`.
    pub(crate) fn open_merged(
        config: PaimonStoreConfig,
        codec: C,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        let mut store = Self::create(config, codec)?;
        if aligned && sources.len() == 1 {
            let (source_dir, snapshot_id) = &sources[0];
            if store.core.adopt_all(source_dir, *snapshot_id)? {
                return Ok(store);
            }
        }
        let write_fields = store.arrow_fields();
        store.core.clip_from_sources(sources, key_groups, &write_fields)?;
        Ok(store)
    }

    fn assemble(core: PaimonTableCore, codec: C) -> Result<Self, DataFusionError> {
        if !codec.supported() {
            return Err(DataFusionError::Plan(
                "state shape not supported by the paimon state backend".into(),
            ));
        }
        let value_fields: Vec<Field> = codec
            .value_fields()
            .into_iter()
            .map(|(name, data_type)| Field::new(name, data_type, true))
            .collect();
        Ok(PaimonMapStore {
            core,
            codec,
            value_fields,
            working: ahash::HashMap::default(),
            footprint: 0,
        })
    }

    fn paimon_schema(
        config: &PaimonStoreConfig,
        codec: &C,
    ) -> Result<PaimonSchema, DataFusionError> {
        let mut builder = PaimonTableCore::schema_builder(config)?.column(
            SUB_KEY_COLUMN,
            PaimonType::VarBinary(
                VarBinaryType::try_new(true, VarBinaryType::MAX_LENGTH).map_err(pe)?,
            ),
        );
        for (name, data_type) in codec.value_fields() {
            let paimon_type = paimon_type_of(&data_type).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "state type {data_type} not supported by the paimon state backend"
                ))
            })?;
            builder = builder.column(name, paimon_type);
        }
        builder
            .primary_key([KG_COLUMN, KEY_COLUMN, SUB_KEY_COLUMN])
            .build()
            .map_err(pe)
    }

    /// The Arrow schema of persisted rows (also the write-batch schema, which additionally
    /// carries `_VALUE_KIND`).
    fn arrow_fields(&self) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KG_COLUMN, DataType::Int32, false),
            Field::new(KEY_COLUMN, DataType::Binary, false),
            Field::new(SUB_KEY_COLUMN, DataType::Binary, false),
        ];
        fields.extend(self.value_fields.iter().cloned());
        fields
    }

    /// Reads the missed keys from the committed table. Entries are collected across ALL probe
    /// batches before assembly — the merge reader may split one key's rows across batch
    /// boundaries.
    fn fetch_missing(&mut self, misses: Vec<ByteKey>) -> Result<(), DataFusionError> {
        let batches = self.core.scan_keys(&misses)?;
        let mut collected: ahash::HashMap<ByteKey, Vec<Vec<ScalarValue>>> =
            ahash::HashMap::default();
        for batch in &batches {
            let expected = self.arrow_fields();
            let keys = normalized_column(batch, 1, &expected[1])?;
            let keys = keys
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| DataFusionError::Internal("paimon key column".into()))?;
            let mut value_columns: Vec<ArrayRef> = Vec::with_capacity(self.value_fields.len());
            for i in 0..self.value_fields.len() {
                value_columns.push(normalized_column(batch, 3 + i, &expected[3 + i])?);
            }
            for row in 0..batch.num_rows() {
                let key = keys.value(row);
                // A key already in the working set stays authoritative over the table.
                if self.working.contains_key(key) {
                    continue;
                }
                let mut scalars: Vec<ScalarValue> = Vec::with_capacity(value_columns.len());
                for column in &value_columns {
                    scalars.push(
                        ScalarValue::try_from_array(column, row)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?,
                    );
                }
                collected.entry(ByteKey::from(key)).or_default().push(scalars);
            }
        }
        let mut added_bytes = 0usize;
        for (key, rows) in collected {
            let mut entries: ahash::HashMap<ByteKey, C::Entry> = ahash::HashMap::default();
            for scalars in &rows {
                let (row_bytes, entry) = self.codec.decode(scalars);
                added_bytes += self.codec.entry_bytes(&row_bytes.0, &entry);
                entries.insert(row_bytes, entry);
            }
            // The image shares the live map's key Arcs; only the entry copies are new.
            let persisted = entries.clone();
            added_bytes += persisted.len() * Self::IMAGE_ENTRY_BYTES;
            added_bytes += byte_key_bytes(&key.0) + Self::SLOT_OVERHEAD;
            self.working.insert(
                key,
                MapSlot::Present { entries, dirty: false, persisted },
            );
        }
        for key in misses {
            self.working.entry(key).or_insert_with(|| {
                added_bytes += Self::SLOT_OVERHEAD;
                MapSlot::Absent { dirty: false, persisted: ahash::HashMap::default() }
            });
        }
        self.footprint += added_bytes as isize;
        Ok(())
    }

    /// Builds the write batch for all dirty slots: one upsert per live entry that differs from
    /// the hydrated image (per-entry dirty — a hot bucket's untouched rows write nothing), one
    /// tombstone per hydrated row no longer present. At most one row per `(k, r)` per checkpoint
    /// by construction (upserts are live, tombstones are not) — required, since within one commit
    /// equal-PK rows resolve by arrival order, which iteration here does not define.
    pub(crate) fn dirty_batch(&self) -> Option<RecordBatch> {
        let num_value = self.value_fields.len();
        let mut kgs: Vec<i32> = Vec::new();
        let mut keys: Vec<&[u8]> = Vec::new();
        let mut subs: Vec<Vec<u8>> = Vec::new();
        let mut values: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_value];
        let mut kinds: Vec<i8> = Vec::new();
        for (key, slot) in self.working.iter() {
            let (entries, persisted, dirty) = match slot {
                MapSlot::Present { entries, persisted, dirty } => (Some(entries), persisted, *dirty),
                MapSlot::Absent { persisted, dirty } => (None, persisted, *dirty),
            };
            if !dirty {
                continue;
            }
            let kg = self.core.key_group(&key.0);
            if let Some(entries) = entries {
                for (row, entry) in entries.iter() {
                    if persisted.get(&*row.0) == Some(entry) {
                        continue; // unchanged since hydration — the table already holds it
                    }
                    kgs.push(kg);
                    keys.push(&key.0);
                    subs.push(self.codec.sub_key(&row.0));
                    for (column, scalar) in values.iter_mut().zip(self.codec.encode(&row.0, entry))
                    {
                        column.push(scalar);
                    }
                    kinds.push(0); // +I upsert — deduplicate keeps the latest by sequence
                }
            }
            for row in persisted.keys() {
                if entries.is_some_and(|entries| entries.contains_key(&*row.0)) {
                    continue;
                }
                kgs.push(kg);
                keys.push(&key.0);
                subs.push(self.codec.sub_key(&row.0));
                for (column, field) in values.iter_mut().zip(self.value_fields.iter()) {
                    column.push(null_scalar(field.data_type()));
                }
                kinds.push(3); // -D tombstone for a vanished row
            }
        }
        if keys.is_empty() {
            return None;
        }
        let mut fields = self.arrow_fields();
        fields.push(Field::new(VALUE_KIND_COLUMN, DataType::Int8, false));
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(kgs)),
            Arc::new(BinaryArray::from_iter_values(keys)),
            Arc::new(BinaryArray::from_iter_values(subs.iter().map(|s| s.as_slice()))),
        ];
        for (i, field) in self.value_fields.iter().enumerate() {
            columns.push(scalars_to_array(std::mem::take(&mut values[i]), field.data_type()));
        }
        columns.push(Arc::new(Int8Array::from(kinds)));
        Some(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("paimon map dirty write batch"),
        )
    }

    /// Checkpoint sync phase, called at the barrier; see the single-value store's `checkpoint`.
    pub(crate) fn checkpoint(&mut self) -> Result<PaimonCheckpointManifest, DataFusionError> {
        self.core.refresh_to_latest()?;
        if let Some(batch) = self.dirty_batch() {
            self.core.commit(&batch)?;
        }
        let footprint = &mut self.footprint;
        let codec = &self.codec;
        self.working.retain(|key, slot| {
            match slot {
                MapSlot::Present { entries, persisted, .. } => {
                    *footprint -= (byte_key_bytes(&key.0)
                        + entries.iter().map(|(r, e)| codec.entry_bytes(&r.0, e)).sum::<usize>()
                        + persisted.len() * Self::IMAGE_ENTRY_BYTES
                        + Self::SLOT_OVERHEAD) as isize;
                }
                MapSlot::Absent { persisted, .. } => {
                    *footprint -=
                        (persisted.len() * Self::IMAGE_ENTRY_BYTES + Self::SLOT_OVERHEAD) as isize;
                }
            }
            false
        });
        self.core.checkpoint_manifest()
    }
}

const RT_COLUMN: &str = "rt";
const FIRED_COLUMN: &str = "fired";

/// What the store knows about a key when a row for it arrives.
pub(crate) enum KeepFirstStatus {
    /// No pending candidate and never fired: the arriving row becomes the candidate.
    Fresh,
    /// A pending candidate with this rowtime (epoch millis) awaits its watermark.
    Pending(i64),
    /// The key's first row was already emitted; every later row for it is ignored.
    Fired,
}

/// Time-buffered keyed state for keep-first rowtime dedup — the first watermark-driven consumer
/// of the dirty region + range overlay. One table row per key under PK `[kg, k]`: the rowtime
/// (millis), a `fired` flag, and the candidate row's payload as typed columns. Pending state is a
/// candidate with `fired = false`; firing upserts the marker row (`fired = true`, payload nulled)
/// so a key's emitted-ness persists on disk — a later row for a fired key can arrive with a
/// rowtime above the watermark (not late), and only the marker stops a duplicate emission. Unlike
/// the point-access stores there is no value codec: payload moves as Arrow columns end to end
/// (input batch → dirty region → barrier flush, committed scan → emission), never through
/// scalars.
///
/// Firing is the overlay range read: committed rows under `rt <= watermark AND NOT fired`
/// (stats-pruned, exact at decode), minus rows shadowed by an uncommitted version of the same key
/// (a DataFusion right-anti hash join against the region's touched keys), plus the region's own
/// live pending rows in range — then an exact per-row re-check, since only the region's rows are
/// guaranteed pre-filtered.
pub(crate) struct PaimonKeepFirstStore {
    core: PaimonTableCore,
    payload_fields: Vec<Field>,
    region: DirtyRegion,
    /// Bundle-scoped committed point-probe results: `Some((rt, fired))` or `None` (absent on
    /// disk). Dropped at `end_bundle`; the region stays authoritative over it.
    probed: ahash::HashMap<ByteKey, Option<(i64, bool)>>,
    last_footprint: usize,
}

impl PaimonKeepFirstStore {
    const PROBE_ENTRY_BYTES: usize =
        std::mem::size_of::<(ByteKey, Option<(i64, bool)>)>() + GROUP_ENTRY_OVERHEAD;

    fn value_fields(payload_fields: &[Field]) -> Vec<Field> {
        let mut fields = vec![
            Field::new(RT_COLUMN, DataType::Int64, true),
            Field::new(FIRED_COLUMN, DataType::Boolean, true),
        ];
        fields.extend(payload_fields.iter().cloned());
        fields
    }

    pub(crate) fn create(
        config: PaimonStoreConfig,
        payload_types: Vec<DataType>,
    ) -> Result<Self, DataFusionError> {
        let payload_fields = Self::payload_fields(&payload_types)?;
        let mut builder = PaimonTableCore::schema_builder(&config)?
            .column(RT_COLUMN, PaimonType::BigInt(BigIntType::new()))
            .column(FIRED_COLUMN, PaimonType::Boolean(BooleanType::new()));
        for field in &payload_fields {
            let paimon_type = paimon_type_of(field.data_type()).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "state type {} not supported by the paimon state backend",
                    field.data_type()
                ))
            })?;
            builder = builder.column(field.name(), paimon_type);
        }
        let schema = builder.primary_key([KG_COLUMN, KEY_COLUMN]).build().map_err(pe)?;
        Self::assemble(PaimonTableCore::create(config, schema)?, payload_fields)
    }

    pub(crate) fn open(
        config: PaimonStoreConfig,
        payload_types: Vec<DataType>,
        snapshot_id: i64,
    ) -> Result<Self, DataFusionError> {
        let payload_fields = Self::payload_fields(&payload_types)?;
        Self::assemble(PaimonTableCore::open(config, snapshot_id)?, payload_fields)
    }

    pub(crate) fn open_merged(
        config: PaimonStoreConfig,
        payload_types: Vec<DataType>,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        let mut store = Self::create(config, payload_types)?;
        if aligned && sources.len() == 1 {
            let (source_dir, snapshot_id) = &sources[0];
            if store.core.adopt_all(source_dir, *snapshot_id)? {
                return Ok(store);
            }
        }
        let write_fields = store.arrow_fields();
        store.core.clip_from_sources(sources, key_groups, &write_fields)?;
        Ok(store)
    }

    fn payload_fields(payload_types: &[DataType]) -> Result<Vec<Field>, DataFusionError> {
        if !paimon_row_supported(payload_types) {
            return Err(DataFusionError::Plan(
                "state shape not supported by the paimon state backend".into(),
            ));
        }
        Ok(payload_types
            .iter()
            .enumerate()
            .map(|(i, t)| Field::new(format!("c{i}"), t.clone(), true))
            .collect())
    }

    fn assemble(
        core: PaimonTableCore,
        payload_fields: Vec<Field>,
    ) -> Result<Self, DataFusionError> {
        let region = DirtyRegion::new(Self::value_fields(&payload_fields), Some(0));
        Ok(PaimonKeepFirstStore {
            core,
            payload_fields,
            region,
            probed: ahash::HashMap::default(),
            last_footprint: 0,
        })
    }

    /// The persisted row schema (also the clip write schema): `kg`, `k`, `rt`, `fired`, payload.
    fn arrow_fields(&self) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KG_COLUMN, DataType::Int32, false),
            Field::new(KEY_COLUMN, DataType::Binary, false),
        ];
        fields.extend(Self::value_fields(&self.payload_fields));
        fields
    }

    /// Fetches `(rt, fired)` for every given key the region and this bundle's probes say nothing
    /// about — one committed point-read join per batch, recording absent keys too.
    pub(crate) fn ensure_probed(&mut self, keys: &[ByteKey]) -> Result<(), DataFusionError> {
        let misses: Vec<ByteKey> = keys
            .iter()
            .filter(|k| !self.region.contains(&k.0) && !self.probed.contains_key(&*k.0))
            .cloned()
            .collect();
        if misses.is_empty() {
            return Ok(());
        }
        let expected = self.arrow_fields();
        for batch in self.core.scan_keys(&misses)? {
            let ks = normalized_column(&batch, 1, &expected[1])?;
            let ks = ks
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| DataFusionError::Internal("paimon key column".into()))?;
            let rts = normalized_column(&batch, 2, &expected[2])?;
            let rts = rts
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("paimon rt column".into()))?;
            let fireds = normalized_column(&batch, 3, &expected[3])?;
            let fireds = fireds
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| DataFusionError::Internal("paimon fired column".into()))?;
            for row in 0..batch.num_rows() {
                self.probed.insert(
                    ByteKey::from(ks.value(row)),
                    Some((rts.value(row), fireds.value(row))),
                );
            }
        }
        for key in misses {
            self.probed.entry(key).or_insert(None);
        }
        Ok(())
    }

    /// The key's status, answered from the region first (uncommitted state wins), then this
    /// bundle's committed probes. Callers must have run `ensure_probed` for the key this bundle.
    pub(crate) fn status(&self, key: &[u8]) -> KeepFirstStatus {
        if let Some(value) = self.region.get(key) {
            return match value {
                crate::state::dirty_region::DirtyValue::Deleted => KeepFirstStatus::Fresh,
                crate::state::dirty_region::DirtyValue::Row(batch, row) => {
                    let fired = batch
                        .column(3)
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .expect("region fired column");
                    if fired.value(row) {
                        KeepFirstStatus::Fired
                    } else {
                        let rts = batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("region rt column");
                        KeepFirstStatus::Pending(rts.value(row))
                    }
                }
            };
        }
        match self.probed.get(key) {
            Some(Some((_, true))) => KeepFirstStatus::Fired,
            Some(Some((rt, false))) => KeepFirstStatus::Pending(*rt),
            _ => KeepFirstStatus::Fresh,
        }
    }

    /// Stages new/improved candidates: one row per key with its rowtime and payload columns.
    pub(crate) fn stage(
        &mut self,
        keys: &[&[u8]],
        rts: Vec<i64>,
        payload: Vec<ArrayRef>,
    ) -> Result<(), DataFusionError> {
        let key_groups: Vec<i32> = keys.iter().map(|key| self.core.key_group(key)).collect();
        let mut values: Vec<ArrayRef> = Vec::with_capacity(2 + payload.len());
        values.push(Arc::new(Int64Array::from(rts)));
        values.push(Arc::new(arrow::array::BooleanArray::from(vec![false; keys.len()])));
        values.extend(payload);
        self.region.append_upserts(keys, &key_groups, values)
    }

    /// Fires every pending candidate whose rowtime the watermark has reached: the overlay range
    /// read (see the type docs), returning region-schema batches (`kg`, `k`, `rt`, `fired`,
    /// payload) of exactly the firing rows, and staging their `fired` markers.
    pub(crate) fn fire(
        &mut self,
        watermark: i64,
        ctx: Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let mut rows: Vec<RecordBatch> = Vec::new();
        let committed = self.committed_in_range(watermark)?;
        if !committed.is_empty() {
            if self.region.is_empty() {
                rows = committed;
            } else {
                let keys: Vec<&[u8]> = self.region.touched_keys().map(|k| k.0.as_ref()).collect();
                let keys_batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("k", DataType::Binary, false)])),
                    vec![Arc::new(BinaryArray::from_iter_values(keys))],
                )
                .expect("anti-join key batch");
                rows = crate::join_common::hash_join_right_anti(
                    keys_batch,
                    committed,
                    &[(0, 1)],
                    ctx,
                )?;
            }
        }
        rows.extend(self.region.live_upserts(Some((i64::MIN, watermark)))?);
        // Exact re-check on every row (committed pushdown is best-effort; region rows still
        // carry fired markers in time range).
        let mut firing: Vec<RecordBatch> = Vec::new();
        let mut fired_keys: Vec<Vec<u8>> = Vec::new();
        let mut fired_rts: Vec<i64> = Vec::new();
        let expected_fields = {
            let mut fields = vec![
                Field::new(KG_COLUMN, DataType::Int32, false),
                Field::new(KEY_COLUMN, DataType::Binary, false),
            ];
            fields.extend(Self::value_fields(&self.payload_fields));
            fields
        };
        for batch in rows {
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(expected_fields.len());
            for (i, field) in expected_fields.iter().enumerate() {
                columns.push(normalized_column(&batch, i, field)?);
            }
            let rts = columns[2]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("overlay rt column".into()))?;
            let fireds = columns[3]
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| DataFusionError::Internal("overlay fired column".into()))?;
            let mask: BooleanArray = (0..batch.num_rows())
                .map(|row| {
                    Some(!fireds.is_valid(row) || !fireds.value(row))
                        .map(|not_fired| not_fired && rts.is_valid(row) && rts.value(row) <= watermark)
                })
                .collect();
            let normalized = RecordBatch::try_new(
                Arc::new(Schema::new(expected_fields.clone())),
                columns,
            )
            .expect("overlay normalized batch");
            let filtered = filter_record_batch(&normalized, &mask)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            if filtered.num_rows() == 0 {
                continue;
            }
            let ks = filtered
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("overlay key column");
            let rts = filtered
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("overlay rt column");
            for row in 0..filtered.num_rows() {
                fired_keys.push(ks.value(row).to_vec());
                fired_rts.push(rts.value(row));
            }
            firing.push(filtered);
        }
        if !fired_keys.is_empty() {
            let key_slices: Vec<&[u8]> = fired_keys.iter().map(|k| k.as_slice()).collect();
            let key_groups: Vec<i32> =
                key_slices.iter().map(|key| self.core.key_group(key)).collect();
            let mut values: Vec<ArrayRef> = Vec::with_capacity(2 + self.payload_fields.len());
            values.push(Arc::new(Int64Array::from(fired_rts)));
            values.push(Arc::new(arrow::array::BooleanArray::from(vec![
                true;
                key_slices.len()
            ])));
            for field in &self.payload_fields {
                values.push(new_null_array(field.data_type(), key_slices.len()));
            }
            self.region.append_upserts(&key_slices, &key_groups, values)?;
        }
        Ok(firing)
    }

    fn committed_in_range(&mut self, watermark: i64) -> Result<Vec<RecordBatch>, DataFusionError> {
        let builder = PredicateBuilder::new(&self.core.fields);
        let predicate = Predicate::and(vec![
            builder
                .less_or_equal(RT_COLUMN, Datum::Long(watermark))
                .map_err(pe)?,
            builder
                .equal(FIRED_COLUMN, Datum::Bool(false))
                .map_err(pe)?,
        ]);
        self.core.scan_predicate(predicate)
    }

    /// Marks the end of the operator's bundle: committed point-probe results drop; only the
    /// region (the write buffer) survives.
    pub(crate) fn end_bundle(&mut self) {
        self.probed.clear();
    }

    /// The store's untracked footprint change since the last call.
    pub(crate) fn footprint_delta(&mut self) -> isize {
        let current = self.region.heap_bytes()
            + self
                .probed
                .keys()
                .map(|k| k.0.len() + Self::PROBE_ENTRY_BYTES)
                .sum::<usize>();
        let delta = current as isize - self.last_footprint as isize;
        self.last_footprint = current;
        delta
    }

    /// Checkpoint sync phase, called at the barrier: commit the region's live rows as the
    /// checkpoint's snapshot and run the checkpoint file phase.
    pub(crate) fn checkpoint(&mut self) -> Result<PaimonCheckpointManifest, DataFusionError> {
        self.core.refresh_to_latest()?;
        let batches = self.region.flush_batches()?;
        if !batches.is_empty() {
            self.core.commit_batches(&batches)?;
        }
        self.region.clear();
        self.probed.clear();
        self.core.checkpoint_manifest()
    }
}

const WINDOW_END_COLUMN: &str = "we";
const WINDOW_START_COLUMN: &str = "ws";

/// One open window's working buffer: the bounded, sorted top-N rows, and how many rows the
/// committed table holds for the window (the barrier's tombstone range).
pub(crate) struct WindowBuffer {
    pub rows: Vec<crate::join_common::JoinRow>,
    persisted_len: usize,
}

/// Time-buffered keyed state for window Top-N / window dedup (`WindowRank`) — the second
/// range-read consumer. One table row per buffered rank position under PK
/// `[kg, k, we, ws, ord]` (`k` = partition-key bytes, `we`/`ws` = window bounds, `ord` = rank),
/// payload as typed columns. Open windows' buffers live decoded in memory for the checkpoint
/// interval — every touch mutates them (sorted insert + truncate), so they are the write buffer
/// itself, not a cache — staged into the dirty region as whole-buffer rewrites at the barrier.
/// Firing a watermark emits the in-memory buffers it closes plus the committed windows it closes
/// that were never touched this interval (a range scan under `we <= watermark`, minus rows whose
/// rank position the region already deleted — fired earlier this interval), then stages `-D`
/// rows for every fired position so the deletion commits at the next barrier. Late-row
/// protection is the watermark check alone; the watermark itself rides the opaque snapshot
/// token, mirroring the memory path's raw snapshot.
pub(crate) struct PaimonWindowRankStore {
    core: PaimonTableCore,
    payload_fields: Vec<Field>,
    region: DirtyRegion,
    buffers: ahash::HashMap<(i64, i64, ByteKey), WindowBuffer>,
    last_footprint: usize,
}

impl PaimonWindowRankStore {
    const BUFFER_ENTRY_BYTES: usize =
        std::mem::size_of::<((i64, i64, ByteKey), WindowBuffer)>() + GROUP_ENTRY_OVERHEAD;

    fn value_fields(payload_fields: &[Field]) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KEY_COLUMN, DataType::Binary, true),
            Field::new(WINDOW_END_COLUMN, DataType::Int64, true),
            Field::new(WINDOW_START_COLUMN, DataType::Int64, true),
            Field::new(ORD_COLUMN, DataType::Int64, true),
        ];
        fields.extend(payload_fields.iter().cloned());
        fields
    }

    pub(crate) fn create(
        config: PaimonStoreConfig,
        payload_types: Vec<DataType>,
    ) -> Result<Self, DataFusionError> {
        let payload_fields = Self::payload_fields(&payload_types)?;
        let mut builder = PaimonTableCore::schema_builder(&config)?
            .column(WINDOW_END_COLUMN, PaimonType::BigInt(BigIntType::new()))
            .column(WINDOW_START_COLUMN, PaimonType::BigInt(BigIntType::new()))
            .column(ORD_COLUMN, PaimonType::BigInt(BigIntType::new()));
        for field in &payload_fields {
            let paimon_type = paimon_type_of(field.data_type()).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "state type {} not supported by the paimon state backend",
                    field.data_type()
                ))
            })?;
            builder = builder.column(field.name(), paimon_type);
        }
        let schema = builder
            .primary_key([KG_COLUMN, KEY_COLUMN, WINDOW_END_COLUMN, WINDOW_START_COLUMN, ORD_COLUMN])
            .build()
            .map_err(pe)?;
        Self::assemble(PaimonTableCore::create(config, schema)?, payload_fields)
    }

    pub(crate) fn open(
        config: PaimonStoreConfig,
        payload_types: Vec<DataType>,
        snapshot_id: i64,
    ) -> Result<Self, DataFusionError> {
        let payload_fields = Self::payload_fields(&payload_types)?;
        Self::assemble(PaimonTableCore::open(config, snapshot_id)?, payload_fields)
    }

    pub(crate) fn open_merged(
        config: PaimonStoreConfig,
        payload_types: Vec<DataType>,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        let mut store = Self::create(config, payload_types)?;
        if aligned && sources.len() == 1 {
            let (source_dir, snapshot_id) = &sources[0];
            if store.core.adopt_all(source_dir, *snapshot_id)? {
                return Ok(store);
            }
        }
        let write_fields = store.arrow_fields();
        store.core.clip_from_sources(sources, key_groups, &write_fields)?;
        Ok(store)
    }

    fn payload_fields(payload_types: &[DataType]) -> Result<Vec<Field>, DataFusionError> {
        if !paimon_row_supported(payload_types) {
            return Err(DataFusionError::Plan(
                "state shape not supported by the paimon state backend".into(),
            ));
        }
        Ok(payload_types
            .iter()
            .enumerate()
            .map(|(i, t)| Field::new(format!("c{i}"), t.clone(), true))
            .collect())
    }

    fn assemble(
        core: PaimonTableCore,
        payload_fields: Vec<Field>,
    ) -> Result<Self, DataFusionError> {
        let region = DirtyRegion::new(Self::value_fields(&payload_fields), Some(1));
        Ok(PaimonWindowRankStore {
            core,
            payload_fields,
            region,
            buffers: ahash::HashMap::default(),
            last_footprint: 0,
        })
    }

    /// The persisted row schema (also the clip write schema).
    fn arrow_fields(&self) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KG_COLUMN, DataType::Int32, false),
            Field::new(KEY_COLUMN, DataType::Binary, false),
            Field::new(WINDOW_END_COLUMN, DataType::Int64, false),
            Field::new(WINDOW_START_COLUMN, DataType::Int64, false),
            Field::new(ORD_COLUMN, DataType::Int64, false),
        ];
        fields.extend(self.payload_fields.iter().cloned());
        fields
    }

    /// The dirty region's composite key for one rank position.
    fn composite_key(pk: &[u8], we: i64, ws: i64, ord: i64) -> Vec<u8> {
        let mut key = Vec::with_capacity(pk.len() + 24);
        key.extend_from_slice(pk);
        key.extend_from_slice(&we.to_be_bytes());
        key.extend_from_slice(&ws.to_be_bytes());
        key.extend_from_slice(&ord.to_be_bytes());
        key
    }

    pub(crate) fn buffer_mut(
        &mut self,
        we: i64,
        ws: i64,
        pk: &ByteKey,
    ) -> Option<&mut WindowBuffer> {
        self.buffers.get_mut(&(we, ws, pk.clone()))
    }

    pub(crate) fn buffer_exists(&self, we: i64, ws: i64, pk: &ByteKey) -> bool {
        self.buffers.contains_key(&(we, ws, pk.clone()))
    }

    /// Creates the windows' buffers, seeded with the committed table's rows for them — one
    /// batched point probe for the distinct partition keys — so committed rows precede this
    /// batch's rows (the ROW_NUMBER tie-break is arrival order). Committed positions arrive in
    /// rank (`ord`) order, which is sort order, so appending preserves the sorted invariant.
    pub(crate) fn seed_windows(
        &mut self,
        windows: &[(i64, i64, ByteKey)],
    ) -> Result<(), DataFusionError> {
        let fresh: Vec<(i64, i64, ByteKey)> = windows
            .iter()
            .filter(|w| !self.buffers.contains_key(*w))
            .cloned()
            .collect();
        if fresh.is_empty() {
            return Ok(());
        }
        for window in &fresh {
            self.buffers
                .insert(window.clone(), WindowBuffer { rows: Vec::new(), persisted_len: 0 });
        }
        let mut keys: Vec<ByteKey> = fresh.iter().map(|(_, _, pk)| pk.clone()).collect();
        keys.sort_by(|a, b| a.0.cmp(&b.0));
        keys.dedup_by(|a, b| a.0 == b.0);
        let expected = self.arrow_fields();
        let mut collected: Vec<(i64, i64, ByteKey, i64, crate::join_common::JoinRow)> = Vec::new();
        for batch in self.core.scan_keys(&keys)? {
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(expected.len());
            for (i, field) in expected.iter().enumerate() {
                columns.push(normalized_column(&batch, i, field)?);
            }
            let ks = columns[1]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| DataFusionError::Internal("paimon key column".into()))?;
            let wes = columns[2]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("paimon we column".into()))?;
            let wss = columns[3]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("paimon ws column".into()))?;
            let ords = columns[4]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("paimon ord column".into()))?;
            for row in 0..batch.num_rows() {
                let window = (wes.value(row), wss.value(row), ByteKey::from(ks.value(row)));
                if !fresh.contains(&window) {
                    continue; // another window of a probed key — not touched, stays on disk
                }
                let mut payload: crate::join_common::JoinRow =
                    Vec::with_capacity(self.payload_fields.len());
                for column in &columns[5..] {
                    payload.push(
                        ScalarValue::try_from_array(column, row)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?,
                    );
                }
                collected.push((window.0, window.1, window.2, ords.value(row), payload));
            }
        }
        collected.sort_by(|a, b| a.3.cmp(&b.3));
        for (we, ws, pk, _, payload) in collected {
            let buffer = self.buffers.get_mut(&(we, ws, pk)).expect("seeded window buffer");
            buffer.rows.push(payload);
            buffer.persisted_len += 1;
        }
        Ok(())
    }

    /// Fires every window the watermark closes: the in-memory buffers plus committed windows
    /// untouched this interval, in deterministic `(we, ws, key)` order, returning `(rows, ranks)`
    /// in emission order and staging the fired positions' deletions.
    pub(crate) fn fire(
        &mut self,
        watermark: i64,
    ) -> Result<(Vec<crate::join_common::JoinRow>, Vec<i64>), DataFusionError> {
        // (we, ws, pk) -> (rows in rank order, committed positions to delete)
        let mut firing: std::collections::BTreeMap<
            (i64, i64, ByteKey),
            (Vec<crate::join_common::JoinRow>, usize),
        > = std::collections::BTreeMap::new();
        let ready: Vec<(i64, i64, ByteKey)> = self
            .buffers
            .keys()
            .filter(|(we, _, _)| *we <= watermark)
            .cloned()
            .collect();
        for window in ready {
            let buffer = self.buffers.remove(&window).expect("ready window buffer");
            firing.insert(window, (buffer.rows, buffer.persisted_len));
        }
        // Committed windows the watermark closes that were never touched this interval. Rank
        // positions already deleted this interval (fired earlier) are shadowed by the region.
        if self.core.read_table.is_some() {
            let builder = PredicateBuilder::new(&self.core.fields);
            let predicate = builder
                .less_or_equal(WINDOW_END_COLUMN, Datum::Long(watermark))
                .map_err(pe)?;
            let expected = self.arrow_fields();
            let mut committed: Vec<(i64, i64, ByteKey, i64, crate::join_common::JoinRow)> =
                Vec::new();
            for batch in self.core.scan_predicate(predicate)? {
                let mut columns: Vec<ArrayRef> = Vec::with_capacity(expected.len());
                for (i, field) in expected.iter().enumerate() {
                    columns.push(normalized_column(&batch, i, field)?);
                }
                let ks = columns[1]
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| DataFusionError::Internal("paimon key column".into()))?;
                let wes = columns[2]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("paimon we column".into()))?;
                let wss = columns[3]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("paimon ws column".into()))?;
                let ords = columns[4]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("paimon ord column".into()))?;
                for row in 0..batch.num_rows() {
                    let (we, ws, ord) = (wes.value(row), wss.value(row), ords.value(row));
                    if we > watermark {
                        continue; // pushdown is best-effort
                    }
                    let pk = ks.value(row);
                    if self.region.contains(&Self::composite_key(pk, we, ws, ord)) {
                        continue; // fired earlier this interval — deletion staged
                    }
                    let window = (we, ws, ByteKey::from(pk));
                    if firing.contains_key(&window) {
                        // The window fired from its buffer just now; its committed positions are
                        // already covered by that entry's delete range.
                        continue;
                    }
                    let mut payload: crate::join_common::JoinRow =
                        Vec::with_capacity(self.payload_fields.len());
                    for column in &columns[5..] {
                        payload.push(
                            ScalarValue::try_from_array(column, row)
                                .map_err(|e| DataFusionError::External(Box::new(e)))?,
                        );
                    }
                    committed.push((we, ws, window.2, ord, payload));
                }
            }
            committed.sort_by(|a, b| a.3.cmp(&b.3));
            for (we, ws, pk, _, payload) in committed {
                let entry = firing.entry((we, ws, pk)).or_insert_with(|| (Vec::new(), 0));
                entry.0.push(payload);
                entry.1 += 1;
            }
        }
        // Emit in (we, ws, key) order and stage every fired position's deletion.
        let mut rows: Vec<crate::join_common::JoinRow> = Vec::new();
        let mut ranks: Vec<i64> = Vec::new();
        let mut delete_keys: Vec<Vec<u8>> = Vec::new();
        let mut delete_kgs: Vec<i32> = Vec::new();
        let mut delete_pks: Vec<Vec<u8>> = Vec::new();
        let mut delete_wes: Vec<i64> = Vec::new();
        let mut delete_wss: Vec<i64> = Vec::new();
        let mut delete_ords: Vec<i64> = Vec::new();
        for ((we, ws, pk), (buffer_rows, persisted_len)) in firing {
            let kg = self.core.key_group(&pk.0);
            for ord in 0..buffer_rows.len().max(persisted_len) as i64 {
                delete_keys.push(Self::composite_key(&pk.0, we, ws, ord));
                delete_kgs.push(kg);
                delete_pks.push(pk.0.to_vec());
                delete_wes.push(we);
                delete_wss.push(ws);
                delete_ords.push(ord);
            }
            for (rank, row) in buffer_rows.into_iter().enumerate() {
                rows.push(row);
                ranks.push(rank as i64 + 1);
            }
        }
        if !delete_keys.is_empty() {
            let key_slices: Vec<&[u8]> = delete_keys.iter().map(|k| k.as_slice()).collect();
            let mut values: Vec<ArrayRef> = vec![
                Arc::new(BinaryArray::from_iter_values(
                    delete_pks.iter().map(|k| k.as_slice()),
                )),
                Arc::new(Int64Array::from(delete_wes)),
                Arc::new(Int64Array::from(delete_wss)),
                Arc::new(Int64Array::from(delete_ords)),
            ];
            for field in &self.payload_fields {
                values.push(new_null_array(field.data_type(), key_slices.len()));
            }
            self.region.append_deletes(&key_slices, &delete_kgs, values)?;
        }
        Ok((rows, ranks))
    }

    /// Stages every surviving open window's buffer — a whole-buffer rewrite per touched window,
    /// upserts at `ord = 0..len` and tombstones for vacated committed positions — then commits
    /// the region (fire deletions included) as the checkpoint's snapshot.
    pub(crate) fn checkpoint(&mut self) -> Result<PaimonCheckpointManifest, DataFusionError> {
        self.core.refresh_to_latest()?;
        let buffers = std::mem::take(&mut self.buffers);
        for ((we, ws, pk), buffer) in &buffers {
            let kg = self.core.key_group(&pk.0);
            let len = buffer.rows.len();
            let total = len.max(buffer.persisted_len);
            let mut keys: Vec<Vec<u8>> = Vec::with_capacity(total);
            for ord in 0..total as i64 {
                keys.push(Self::composite_key(&pk.0, *we, *ws, ord));
            }
            let upsert_keys: Vec<&[u8]> = keys[..len].iter().map(|k| k.as_slice()).collect();
            if !upsert_keys.is_empty() {
                let mut values: Vec<ArrayRef> = vec![
                    Arc::new(BinaryArray::from_iter_values(
                        std::iter::repeat_n(pk.0.as_ref(), len),
                    )),
                    Arc::new(Int64Array::from(vec![*we; len])),
                    Arc::new(Int64Array::from(vec![*ws; len])),
                    Arc::new(Int64Array::from((0..len as i64).collect::<Vec<_>>())),
                ];
                for (j, field) in self.payload_fields.iter().enumerate() {
                    values.push(scalars_to_array(
                        buffer.rows.iter().map(|r| r[j].clone()).collect(),
                        field.data_type(),
                    ));
                }
                self.region
                    .append_upserts(&upsert_keys, &vec![kg; len], values)?;
            }
            if buffer.persisted_len > len {
                let vacated = buffer.persisted_len - len;
                let delete_keys: Vec<&[u8]> =
                    keys[len..].iter().map(|k| k.as_slice()).collect();
                let mut values: Vec<ArrayRef> = vec![
                    Arc::new(BinaryArray::from_iter_values(
                        std::iter::repeat_n(pk.0.as_ref(), vacated),
                    )),
                    Arc::new(Int64Array::from(vec![*we; vacated])),
                    Arc::new(Int64Array::from(vec![*ws; vacated])),
                    Arc::new(Int64Array::from(
                        (len as i64..buffer.persisted_len as i64).collect::<Vec<_>>(),
                    )),
                ];
                for field in &self.payload_fields {
                    values.push(new_null_array(field.data_type(), vacated));
                }
                self.region.append_deletes(&delete_keys, &vec![kg; vacated], values)?;
            }
        }
        let flushed = self.region.flush_batches()?;
        if !flushed.is_empty() {
            let mut fields = self.arrow_fields();
            fields.push(Field::new(VALUE_KIND_COLUMN, DataType::Int8, false));
            let write_schema = Arc::new(Schema::new(fields));
            // Reproject: drop the region's composite-key column; its value columns already lead
            // with the table's real key columns.
            let write_batches: Vec<RecordBatch> = flushed
                .iter()
                .map(|batch| {
                    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns() - 1);
                    columns.push(batch.column(0).clone());
                    columns.extend(batch.columns()[2..].iter().cloned());
                    RecordBatch::try_new(write_schema.clone(), columns)
                        .expect("window-rank write batch")
                })
                .collect();
            self.core.commit_batches(&write_batches)?;
        }
        self.region.clear();
        self.core.checkpoint_manifest()
    }

    /// The payload columns as positional Arrow fields — a post-restore emission schema.
    pub(crate) fn payload_fields_for_schema(&self) -> Vec<Field> {
        self.payload_fields.clone()
    }

    /// The store's untracked footprint change since the last call.
    pub(crate) fn footprint_delta(&mut self) -> isize {
        let current = self.region.heap_bytes()
            + self
                .buffers
                .iter()
                .map(|((_, _, pk), buffer)| {
                    pk.0.len()
                        + buffer
                            .rows
                            .iter()
                            .map(|r| scalar_row_bytes(r) + GROUP_ENTRY_OVERHEAD)
                            .sum::<usize>()
                        + Self::BUFFER_ENTRY_BYTES
                })
                .sum::<usize>();
        let delta = current as isize - self.last_footprint as isize;
        self.last_footprint = current;
        delta
    }
}

impl WindowBuffer {
    /// Sorted insert preserving arrival order for ties, then truncate past rank N — the same
    /// discipline as the memory path's buffers.
    pub(crate) fn insert_ranked(
        &mut self,
        row: crate::join_common::JoinRow,
        sort_columns: &[crate::topn::SortColumn],
        limit: usize,
    ) {
        let pos = self.rows.partition_point(|r| {
            crate::topn::compare_rows(r, &row, sort_columns) != std::cmp::Ordering::Greater
        });
        self.rows.insert(pos, row);
        if self.rows.len() > limit {
            self.rows.truncate(limit);
        }
    }
}
