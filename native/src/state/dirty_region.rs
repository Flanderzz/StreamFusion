//! The write buffer as a queryable set: arrival-ordered Arrow batches with liveness bitmaps.
//!
//! The point-access stores hold their write buffer as decoded per-key slots, which answers "what
//! is the state for key k" and nothing else. Watermark/timer operators need the other question —
//! "every buffered row with `t <= watermark`" — and that range result must reflect uncommitted
//! adds and deletes, which a map of slots cannot express. This region holds the uncommitted
//! mutations as Arrow batches instead: mutations append rows (nothing rewrites), a liveness
//! bitmap per batch marks superseded versions, a key index locates each key's live version for
//! point reads, and per-batch min/max on a designated time column prunes range scans the same
//! way parquet row-group stats prune the committed side.
//!
//! At the barrier the live rows flush to the Paimon writer in arrival order. The index keeps at
//! most one live version per key, so the flush emits at most one row per primary key per commit
//! — required, because within one commit equal-PK rows resolve by arrival order.

use crate::*;
use arrow::array::BinaryArray;

const VALUE_KIND_COLUMN: &str = "_VALUE_KIND";

/// A key's current uncommitted version.
pub(crate) enum DirtyValue<'a> {
    /// The key was written this interval; the row holds its live version.
    Row(&'a RecordBatch, usize),
    /// The key was deleted this interval — authoritative absence that must shadow any committed
    /// row for the key.
    Deleted,
}

struct DirtyRef {
    batch: usize,
    row: usize,
    deleted: bool,
}

struct DirtyBatch {
    /// Region-schema rows (`kg`, `k`, value columns); a delete row carries null values.
    batch: RecordBatch,
    /// Per-row value kind: 0 upsert, 3 delete (Paimon `-D`).
    kinds: Vec<i8>,
    /// Liveness: false once a later append superseded the row.
    live: Vec<bool>,
    live_rows: usize,
    /// Min/max of the time column over this batch's *upsert* rows (superseded rows included —
    /// conservative pruning; the liveness filter removes them from results). `min > max` means
    /// the batch holds no upserts and range scans skip it entirely.
    time_min: i64,
    time_max: i64,
}

/// See the module docs. The schema is `[kg, k, <value columns>]`; `time_column` indexes an Int64
/// column within the *value* columns that range scans filter and prune on.
pub(crate) struct DirtyRegion {
    schema: SchemaRef,
    time_column: Option<usize>,
    max_parallelism: usize,
    batches: Vec<DirtyBatch>,
    index: ahash::HashMap<ByteKey, DirtyRef>,
    heap_bytes: usize,
}

impl DirtyRegion {
    const INDEX_ENTRY_BYTES: usize =
        std::mem::size_of::<(ByteKey, DirtyRef)>() + GROUP_ENTRY_OVERHEAD;

    pub(crate) fn new(
        value_fields: Vec<Field>,
        time_column: Option<usize>,
        max_parallelism: usize,
    ) -> Self {
        if let Some(t) = time_column {
            assert_eq!(
                value_fields[t].data_type(),
                &DataType::Int64,
                "dirty region time column must be Int64"
            );
        }
        let mut fields = vec![
            Field::new("kg", DataType::Int32, false),
            Field::new("k", DataType::Binary, false),
        ];
        fields.extend(value_fields);
        DirtyRegion {
            schema: Arc::new(Schema::new(fields)),
            time_column,
            max_parallelism,
            batches: Vec::new(),
            index: ahash::HashMap::default(),
            heap_bytes: 0,
        }
    }

    /// The region-schema rows (`kg`, `k`, values); flush appends `_VALUE_KIND`.
    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Appends one upsert per key, values aligned by position. A key appended twice — in one call
    /// or across calls — keeps only its latest version live.
    pub(crate) fn append_upserts(
        &mut self,
        keys: &[&[u8]],
        values: Vec<ArrayRef>,
    ) -> Result<(), DataFusionError> {
        self.append(keys, values, 0)
    }

    /// Appends one delete per key (null value columns). Deleting a key never committed is
    /// harmless; the delete still shadows any committed row at the next flush.
    pub(crate) fn append_deletes(&mut self, keys: &[&[u8]]) -> Result<(), DataFusionError> {
        let values = self.schema.fields()[2..]
            .iter()
            .map(|f| new_null_array(f.data_type(), keys.len()))
            .collect();
        self.append(keys, values, 3)
    }

    fn append(
        &mut self,
        keys: &[&[u8]],
        values: Vec<ArrayRef>,
        kind: i8,
    ) -> Result<(), DataFusionError> {
        if keys.is_empty() {
            return Ok(());
        }
        let kgs: Int32Array = keys
            .iter()
            .map(|k| {
                Some(flink_key_group(hash_bytes_by_words(k), self.max_parallelism) as i32)
            })
            .collect();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        columns.push(Arc::new(kgs));
        columns.push(Arc::new(BinaryArray::from_iter_values(keys)));
        columns.extend(values);
        let batch = RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let (mut time_min, mut time_max) = (i64::MAX, i64::MIN);
        if kind == 0 {
            if let Some(t) = self.time_column {
                let times = batch
                    .column(2 + t)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("dirty region time column");
                for row in 0..times.len() {
                    let v = times.value(row);
                    time_min = time_min.min(v);
                    time_max = time_max.max(v);
                }
            } else {
                (time_min, time_max) = (i64::MIN, i64::MAX);
            }
        }

        let batch_index = self.batches.len();
        self.heap_bytes += batch.get_array_memory_size();
        for (row, key) in keys.iter().enumerate() {
            match self.index.get_mut(*key) {
                Some(prev) => {
                    // A previous version in an OLDER batch dies now; a version earlier in THIS
                    // batch (a key repeated within one call) is handled by the liveness rebuild
                    // below, since this batch is not pushed yet.
                    if prev.batch < batch_index && !prev.deleted {
                        let superseded = &mut self.batches[prev.batch];
                        superseded.live[prev.row] = false;
                        superseded.live_rows -= 1;
                    }
                    *prev = DirtyRef { batch: batch_index, row, deleted: kind == 3 };
                }
                None => {
                    self.heap_bytes += Self::INDEX_ENTRY_BYTES + key.len();
                    self.index.insert(
                        ByteKey::from(*key),
                        DirtyRef { batch: batch_index, row, deleted: kind == 3 },
                    );
                }
            }
        }
        // Only rows the index still points at are live — a key repeated within this call keeps
        // exactly its last occurrence.
        let mut live = vec![false; keys.len()];
        let mut live_rows = 0usize;
        for (row, key) in keys.iter().enumerate() {
            let entry = self.index.get(*key).expect("just indexed");
            if entry.batch == batch_index && entry.row == row {
                live[row] = true;
                live_rows += 1;
            }
        }
        self.batches.push(DirtyBatch {
            batch,
            kinds: vec![kind; keys.len()],
            live,
            live_rows,
            time_min,
            time_max,
        });
        Ok(())
    }

    /// The key's uncommitted version, if any. `None` means the region says nothing about the key
    /// — the committed table remains authoritative for it.
    pub(crate) fn get(&self, key: &[u8]) -> Option<DirtyValue<'_>> {
        let entry = self.index.get(key)?;
        if entry.deleted {
            return Some(DirtyValue::Deleted);
        }
        Some(DirtyValue::Row(&self.batches[entry.batch].batch, entry.row))
    }

    /// Whether the region holds any version (upsert or delete) for the key.
    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        self.index.contains_key(key)
    }

    /// Every key the region holds a version for — the anti-join set that removes committed rows
    /// shadowed by uncommitted state (deletes included: a deleted key must vanish from results).
    pub(crate) fn touched_keys(&self) -> impl Iterator<Item = &ByteKey> {
        self.index.keys()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Live upsert rows whose time column falls in `[lo, hi]` (the whole region when `None`),
    /// bitmap-filtered, per-batch stats pruned, in arrival order. Region schema, no kind column.
    pub(crate) fn live_upserts(
        &self,
        time_range: Option<(i64, i64)>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let mut out = Vec::new();
        for dirty in &self.batches {
            if dirty.live_rows == 0 || dirty.time_min > dirty.time_max {
                continue; // no live rows, or an all-deletes batch
            }
            if let Some((lo, hi)) = time_range {
                if dirty.time_min > hi || dirty.time_max < lo {
                    continue;
                }
            }
            let mask: BooleanArray = match (self.time_column, time_range) {
                (Some(t), Some((lo, hi))) => {
                    let times = dirty
                        .batch
                        .column(2 + t)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("dirty region time column");
                    (0..dirty.batch.num_rows())
                        .map(|row| {
                            Some(
                                dirty.live[row]
                                    && dirty.kinds[row] == 0
                                    && (lo..=hi).contains(&times.value(row)),
                            )
                        })
                        .collect()
                }
                _ => (0..dirty.batch.num_rows())
                    .map(|row| Some(dirty.live[row] && dirty.kinds[row] == 0))
                    .collect(),
            };
            let filtered = filter_record_batch(&dirty.batch, &mask)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
        }
        Ok(out)
    }

    /// The barrier flush: each batch's live rows — upserts and deletes — with `_VALUE_KIND`
    /// appended, in arrival order. At most one row per key across the result by construction.
    pub(crate) fn flush_batches(&self) -> Result<Vec<RecordBatch>, DataFusionError> {
        let mut fields: Vec<Field> =
            self.schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        fields.push(Field::new(VALUE_KIND_COLUMN, DataType::Int8, false));
        let flush_schema = Arc::new(Schema::new(fields));
        let mut out = Vec::new();
        for dirty in &self.batches {
            if dirty.live_rows == 0 {
                continue;
            }
            let mask: BooleanArray =
                dirty.live.iter().map(|&live| Some(live)).collect();
            let filtered = filter_record_batch(&dirty.batch, &mask)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let kinds: Vec<i8> = dirty
                .kinds
                .iter()
                .zip(&dirty.live)
                .filter(|(_, &live)| live)
                .map(|(&kind, _)| kind)
                .collect();
            let mut columns = filtered.columns().to_vec();
            columns.push(Arc::new(Int8Array::from(kinds)));
            out.push(
                RecordBatch::try_new(flush_schema.clone(), columns)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            );
        }
        Ok(out)
    }

    /// Drops everything — called after the barrier committed the flush.
    pub(crate) fn clear(&mut self) {
        self.batches.clear();
        self.index.clear();
        self.heap_bytes = 0;
    }

    /// The region's accounted heap footprint (batch buffers plus the key index).
    pub(crate) fn heap_bytes(&self) -> usize {
        self.heap_bytes
    }
}
