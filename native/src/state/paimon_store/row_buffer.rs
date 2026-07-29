use super::*;

/// One time-buffered table of whole input rows keyed by an **arrival sequence** — the shared
/// component behind every "rows pending the watermark" state (the OVER aggregate's pending side,
/// each side of a window join). PK `[kg, k]`: `k` is the sequence's big-endian i64, so byte
/// order is arrival order and a firing can merge the write buffer with the committed table back
/// into exactly the memory path's emission order; `kg` is the key group of the row's shuffle key
/// (rescale clips by it). Columns: `rt` — the epoch-milli time the watermark fires on (a rowtime,
/// a window end) — and the full input row as typed payload. Fired rows leave state: a `-D` per
/// row commits at the next barrier. The sequence itself rides the operator's snapshot token;
/// without it a restored subtask's new rows would order ahead of older pending rows.
pub(crate) struct PaimonRowBufferStore {
    core: PaimonTableCore,
    payload_fields: Vec<Field>,
    region: DirtyRegion,
    next_seq: i64,
}

impl PaimonRowBufferStore {
    pub(super) fn typed_fields(
        prefix: &str,
        types: &[DataType],
    ) -> Result<Vec<Field>, DataFusionError> {
        if !paimon_row_supported(types) {
            return Err(DataFusionError::Plan(
                "state shape not supported by the paimon state backend".into(),
            ));
        }
        Ok(types
            .iter()
            .enumerate()
            .map(|(i, t)| Field::new(format!("{prefix}{i}"), t.clone(), true))
            .collect())
    }

    pub(crate) fn create(
        config: PaimonStoreConfig,
        payload_types: Vec<DataType>,
    ) -> Result<Self, DataFusionError> {
        let payload_fields = Self::typed_fields("c", &payload_types)?;
        let mut builder = PaimonTableCore::schema_builder(&config)?
            .column(RT_COLUMN, PaimonType::BigInt(BigIntType::new()));
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
        let region = DirtyRegion::new(Self::value_fields(&payload_fields), Some(0));
        Ok(PaimonRowBufferStore {
            core: PaimonTableCore::create(config, schema)?,
            payload_fields,
            region,
            next_seq: 0,
        })
    }

    /// Restores from checkpoint sources: an aligned single source adopts files wholesale,
    /// anything else clips by key-group range. Empty sources leave a fresh table.
    pub(crate) fn open_merged(
        config: PaimonStoreConfig,
        payload_types: Vec<DataType>,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        let mut store = Self::create(config, payload_types)?;
        if sources.is_empty() {
            return Ok(store);
        }
        if aligned && sources.len() == 1 {
            let (source_dir, snapshot_id) = &sources[0];
            if store.core.adopt_all(source_dir, *snapshot_id)? {
                return Ok(store);
            }
        }
        let write_fields = store.arrow_fields();
        store.core.clip_from_sources(sources, key_groups, &write_fields, crate::state::StateTtl::disabled())?;
        Ok(store)
    }

    fn value_fields(payload_fields: &[Field]) -> Vec<Field> {
        let mut fields = vec![Field::new(RT_COLUMN, DataType::Int64, true)];
        fields.extend(payload_fields.iter().cloned());
        fields
    }

    /// The persisted row schema (also the clip write schema): `kg`, `k`, `rt`, payload.
    fn arrow_fields(&self) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KG_COLUMN, DataType::Int32, false),
            Field::new(KEY_COLUMN, DataType::Binary, false),
        ];
        fields.extend(Self::value_fields(&self.payload_fields));
        fields
    }

    pub(crate) fn key_group(&self, key: &[u8]) -> i32 {
        self.core.key_group(key)
    }

    pub(crate) fn next_seq(&self) -> i64 {
        self.next_seq
    }

    pub(crate) fn set_next_seq(&mut self, seq: i64) {
        self.next_seq = seq;
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        self.region.heap_bytes()
    }

    /// Buffers one input batch's rows, each under a fresh arrival sequence. `kgs` route each row
    /// by its shuffle key; `times` are the epoch millis the watermark fires on.
    pub(crate) fn stage(
        &mut self,
        kgs: &[i32],
        times: Vec<i64>,
        payload: Vec<ArrayRef>,
    ) -> Result<(), DataFusionError> {
        let n = kgs.len();
        if n == 0 {
            return Ok(());
        }
        let keys_owned: Vec<[u8; 8]> =
            (0..n).map(|i| (self.next_seq + i as i64).to_be_bytes()).collect();
        self.next_seq += n as i64;
        let key_slices: Vec<&[u8]> = keys_owned.iter().map(|k| k.as_slice()).collect();
        let mut values: Vec<ArrayRef> = Vec::with_capacity(1 + payload.len());
        values.push(Arc::new(Int64Array::from(times)));
        values.extend(payload);
        self.region.append_upserts(&key_slices, kgs, values)
    }

    /// Every buffered row the watermark completed — the overlay range read (committed rows under
    /// `time ≤ watermark` minus rows the region already touched, plus the region's live rows in
    /// range) — merged back into arrival order by the sequence key. Store schema (`kg`, `k`,
    /// `rt`, payload…), `None` when nothing fired. Fired rows leave state: their deletions stage
    /// into the region.
    pub(crate) fn fire(
        &mut self,
        watermark: i64,
        ctx: Arc<TaskContext>,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        let mut rows: Vec<RecordBatch> = Vec::new();
        let committed = {
            let builder = PredicateBuilder::new(&self.core.fields);
            let predicate = builder
                .less_or_equal(RT_COLUMN, Datum::Long(watermark))
                .map_err(pe)?;
            self.core.scan_predicate(predicate)?
        };
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
                rows = crate::join_common::hash_join_right_anti(keys_batch, committed, &[(0, 1)], ctx)?;
            }
        }
        rows.extend(self.region.live_upserts(Some((i64::MIN, watermark)))?);
        // Exact re-check per row (committed pushdown is best-effort), normalizing reader types.
        let expected = self.arrow_fields();
        let mut normalized: Vec<RecordBatch> = Vec::new();
        for batch in rows {
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(expected.len());
            for (i, field) in expected.iter().enumerate() {
                columns.push(normalized_column(&batch, i, field)?);
            }
            let rts = columns[2]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("overlay rt column".into()))?;
            let mask: BooleanArray = (0..batch.num_rows())
                .map(|row| Some(rts.is_valid(row) && rts.value(row) <= watermark))
                .collect();
            let normalized_batch =
                RecordBatch::try_new(Arc::new(Schema::new(expected.clone())), columns)
                    .expect("overlay normalized batch");
            let filtered = filter_record_batch(&normalized_batch, &mask)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            if filtered.num_rows() > 0 {
                normalized.push(filtered);
            }
        }
        if normalized.is_empty() {
            return Ok(None);
        }
        let merged = concat_batches(&Arc::new(Schema::new(expected)), &normalized)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        // The sequence key is big-endian and non-negative, so byte order is arrival order.
        let ks = merged
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| DataFusionError::Internal("overlay seq column".into()))?;
        let mut order: Vec<u32> = (0..merged.num_rows() as u32).collect();
        order.sort_by_key(|&row| ks.value(row as usize));
        let indices = UInt32Array::from(order);
        let columns: Vec<ArrayRef> = merged
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).expect("row buffer fire sort"))
            .collect();
        let sorted = RecordBatch::try_new(merged.schema(), columns).expect("row buffer fired batch");
        {
            let ks = sorted
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("sorted seq column");
            let kgs = sorted
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("sorted kg column");
            let key_slices: Vec<&[u8]> = (0..sorted.num_rows()).map(|row| ks.value(row)).collect();
            let key_groups: Vec<i32> = (0..sorted.num_rows()).map(|row| kgs.value(row)).collect();
            self.region.append_null_deletes(&key_slices, &key_groups)?;
        }
        Ok(Some(sorted))
    }

    /// Checkpoint sync phase: commits the region's live rows and fired deletions as this table's
    /// snapshot and runs the checkpoint file phase.
    pub(crate) fn checkpoint(&mut self) -> Result<PaimonCheckpointManifest, DataFusionError> {
        self.core.refresh_to_latest()?;
        let batches = self.region.flush_batches()?;
        if !batches.is_empty() {
            self.core.commit_batches(&batches)?;
        }
        self.region.clear();
        self.core.checkpoint_manifest()
    }
}
