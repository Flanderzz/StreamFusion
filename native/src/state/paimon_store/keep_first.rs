use super::*;


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
