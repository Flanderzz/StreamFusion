use super::*;

/// Keyed open-window accumulator state for the aligned window aggregates (tumbling / hopping /
/// cumulative) — the fifth range-read consumer. One table row per open (key, window) under PK
/// `[kg, k, we, ws]` (`k` = grouping-key BinaryRow, `we`/`ws` = window bounds in epoch millis),
/// carrying the decoded key columns (emission needs them typed) and every accumulator's state
/// fields — the same scalars the raw snapshot round-trips. Open windows' accumulators live
/// decoded in the operator's memory for the checkpoint interval — every row touches them, so
/// they are the write buffer — staged as whole-row rewrites at the barrier and dropped from
/// memory (a later touch re-seeds from the committed table through the per-batch key probe,
/// once per key per interval: the table is immutable between barriers). A watermark firing
/// hydrates the committed windows it closes (minus rows the region already deleted — fired
/// earlier this interval), lets the memory path drain them, and stages a `-D` per fired row.
/// The current watermark rides the opaque snapshot token, as the memory path persists it in its
/// raw snapshot (late-data dropping must survive restore).
pub(crate) struct PaimonWindowAggStore {
    core: PaimonTableCore,
    key_fields: Vec<Field>,
    state_fields: Vec<Field>,
    region: DirtyRegion,
    /// Keys whose committed windows already seeded the operator's memory this interval.
    seeded: StdHashSet<ByteKey>,
    last_footprint: usize,
}

impl PaimonWindowAggStore {
    const SEEDED_ENTRY_BYTES: usize = std::mem::size_of::<ByteKey>() + GROUP_ENTRY_OVERHEAD;

    /// The region's value columns: the table's PK components after `kg` (the BinaryRow key and
    /// the window bounds — a `-D` must address its row), then the typed key and state columns.
    fn value_fields(key_fields: &[Field], state_fields: &[Field]) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KEY_COLUMN, DataType::Binary, false),
            Field::new(WINDOW_END_COLUMN, DataType::Int64, false),
            Field::new(WINDOW_START_COLUMN, DataType::Int64, false),
        ];
        fields.extend(key_fields.iter().cloned());
        fields.extend(state_fields.iter().cloned());
        fields
    }

    pub(crate) fn create(
        config: PaimonStoreConfig,
        key_types: Vec<DataType>,
        state_types: Vec<DataType>,
    ) -> Result<Self, DataFusionError> {
        let key_fields = PaimonRowBufferStore::typed_fields("g", &key_types)?;
        let state_fields = PaimonRowBufferStore::typed_fields("s", &state_types)?;
        let mut builder = PaimonTableCore::schema_builder(&config)?
            .column(WINDOW_END_COLUMN, PaimonType::BigInt(BigIntType::new()))
            .column(WINDOW_START_COLUMN, PaimonType::BigInt(BigIntType::new()));
        for field in key_fields.iter().chain(&state_fields) {
            let paimon_type = paimon_type_of(field.data_type()).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "state type {} not supported by the paimon state backend",
                    field.data_type()
                ))
            })?;
            builder = builder.column(field.name(), paimon_type);
        }
        let schema = builder
            .primary_key([KG_COLUMN, KEY_COLUMN, WINDOW_END_COLUMN, WINDOW_START_COLUMN])
            .build()
            .map_err(pe)?;
        let region =
            DirtyRegion::new(Self::value_fields(&key_fields, &state_fields), Some(1));
        Ok(PaimonWindowAggStore {
            core: PaimonTableCore::create(config, schema)?,
            key_fields,
            state_fields,
            region,
            seeded: StdHashSet::new(),
            last_footprint: 0,
        })
    }

    pub(crate) fn open_merged(
        config: PaimonStoreConfig,
        key_types: Vec<DataType>,
        state_types: Vec<DataType>,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        let mut store = Self::create(config, key_types, state_types)?;
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

    /// The persisted row schema (also the clip write schema).
    fn arrow_fields(&self) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KG_COLUMN, DataType::Int32, false),
            Field::new(KEY_COLUMN, DataType::Binary, false),
        ];
        fields.extend(Self::value_fields(&self.key_fields, &self.state_fields)[1..].to_vec());
        fields
    }

    pub(crate) fn key_group(&self, key: &[u8]) -> i32 {
        self.core.key_group(key)
    }

    fn composite_key(key: &[u8], we: i64, ws: i64) -> Vec<u8> {
        let mut composite = Vec::with_capacity(key.len() + 16);
        composite.extend_from_slice(key);
        composite.extend_from_slice(&we.to_be_bytes());
        composite.extend_from_slice(&ws.to_be_bytes());
        composite
    }

    /// The committed open windows of every given key not yet seeded this interval, as normalized
    /// store-schema batches (`kg`, `k`, `we`, `ws`, keys…, states…), minus rows the region
    /// already deleted — a window fired from the committed table earlier this interval must not
    /// re-seed when its key is touched afterwards. Marks the keys seeded — the committed table
    /// is immutable between barriers, so one probe per key per interval.
    pub(crate) fn seed_scan(
        &mut self,
        keys: &[ByteKey],
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let misses: Vec<ByteKey> =
            keys.iter().filter(|k| !self.seeded.contains(*k)).cloned().collect();
        if misses.is_empty() {
            return Ok(Vec::new());
        }
        for key in &misses {
            self.seeded.insert(key.clone());
        }
        let batches = self.core.scan_keys(&misses)?;
        let normalized = self.normalize(batches)?;
        self.filter_region_deleted(normalized, None)
    }

    /// The committed rows of every window the watermark closes, minus rows the region already
    /// deleted (fired earlier this interval), normalized. The caller skips windows it already
    /// holds decoded in memory — those are authoritative.
    pub(crate) fn fire_scan(
        &mut self,
        watermark: i64,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let committed = {
            let builder = PredicateBuilder::new(&self.core.fields);
            let predicate = builder
                .less_or_equal(WINDOW_END_COLUMN, Datum::Long(watermark))
                .map_err(pe)?;
            self.core.scan_predicate(predicate)?
        };
        let normalized = self.normalize(committed)?;
        self.filter_region_deleted(normalized, Some(watermark))
    }

    /// Drops rows whose (key, window) the region already deleted — fired earlier this interval —
    /// and, when a watermark is given, re-checks the pushed range predicate exactly.
    fn filter_region_deleted(
        &self,
        batches: Vec<RecordBatch>,
        watermark: Option<i64>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let mut out = Vec::new();
        for batch in batches {
            let ks = batch.column(1).as_any().downcast_ref::<BinaryArray>().expect("k column");
            let wes = batch.column(2).as_any().downcast_ref::<Int64Array>().expect("we column");
            let wss = batch.column(3).as_any().downcast_ref::<Int64Array>().expect("ws column");
            let mask: BooleanArray = (0..batch.num_rows())
                .map(|row| {
                    let (we, ws) = (wes.value(row), wss.value(row));
                    Some(
                        watermark.is_none_or(|wm| we <= wm)
                            && !self
                                .region
                                .contains(&Self::composite_key(ks.value(row), we, ws)),
                    )
                })
                .collect();
            let filtered = filter_record_batch(&batch, &mask)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
        }
        Ok(out)
    }

    fn normalize(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let expected = self.arrow_fields();
        let schema = Arc::new(Schema::new(expected.clone()));
        let mut out = Vec::with_capacity(batches.len());
        for batch in batches {
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(expected.len());
            for (i, field) in expected.iter().enumerate() {
                columns.push(normalized_column(&batch, i, field)?);
            }
            out.push(
                RecordBatch::try_new(schema.clone(), columns).expect("window-agg normalized batch"),
            );
        }
        Ok(out)
    }

    /// Stages one row per open (key, window) — the barrier's whole-state rewrite.
    pub(crate) fn stage_upserts(
        &mut self,
        keys: &[&[u8]],
        wes: &[i64],
        wss: &[i64],
        key_columns: Vec<ArrayRef>,
        state_columns: Vec<ArrayRef>,
    ) -> Result<(), DataFusionError> {
        if keys.is_empty() {
            return Ok(());
        }
        let composites: Vec<Vec<u8>> = keys
            .iter()
            .zip(wes.iter().zip(wss))
            .map(|(key, (&we, &ws))| Self::composite_key(key, we, ws))
            .collect();
        let composite_slices: Vec<&[u8]> = composites.iter().map(|k| k.as_slice()).collect();
        let key_groups: Vec<i32> = keys.iter().map(|key| self.core.key_group(key)).collect();
        let mut values: Vec<ArrayRef> =
            Vec::with_capacity(3 + key_columns.len() + state_columns.len());
        values.push(Arc::new(BinaryArray::from_iter_values(keys)));
        values.push(Arc::new(Int64Array::from(wes.to_vec())));
        values.push(Arc::new(Int64Array::from(wss.to_vec())));
        values.extend(key_columns);
        values.extend(state_columns);
        self.region.append_upserts(&composite_slices, &key_groups, values)
    }

    /// Stages a `-D` per fired (key, window); the tombstone carries its PK components.
    pub(crate) fn stage_deletes(
        &mut self,
        keys: &[&[u8]],
        wes: &[i64],
        wss: &[i64],
    ) -> Result<(), DataFusionError> {
        if keys.is_empty() {
            return Ok(());
        }
        let composites: Vec<Vec<u8>> = keys
            .iter()
            .zip(wes.iter().zip(wss))
            .map(|(key, (&we, &ws))| Self::composite_key(key, we, ws))
            .collect();
        let composite_slices: Vec<&[u8]> = composites.iter().map(|k| k.as_slice()).collect();
        let key_groups: Vec<i32> = keys.iter().map(|key| self.core.key_group(key)).collect();
        let mut values: Vec<ArrayRef> =
            Vec::with_capacity(3 + self.key_fields.len() + self.state_fields.len());
        values.push(Arc::new(BinaryArray::from_iter_values(keys)));
        values.push(Arc::new(Int64Array::from(wes.to_vec())));
        values.push(Arc::new(Int64Array::from(wss.to_vec())));
        for field in self.key_fields.iter().chain(&self.state_fields) {
            values.push(new_null_array(field.data_type(), keys.len()));
        }
        self.region.append_deletes(&composite_slices, &key_groups, values)
    }

    /// The store's untracked footprint change since the last call.
    pub(crate) fn footprint_delta(&mut self) -> isize {
        let current = self.region.heap_bytes()
            + self
                .seeded
                .iter()
                .map(|k| k.0.len() + Self::SEEDED_ENTRY_BYTES)
                .sum::<usize>();
        let delta = current as isize - self.last_footprint as isize;
        self.last_footprint = current;
        delta
    }

    /// Checkpoint sync phase: commits the region (staged open windows and fired deletions) as
    /// the checkpoint's snapshot. The caller stages the open windows and clears its decoded map
    /// first; the seeded set resets — the next interval re-probes on demand.
    pub(crate) fn checkpoint(&mut self) -> Result<PaimonCheckpointManifest, DataFusionError> {
        self.core.refresh_to_latest()?;
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
                        .expect("window-agg write batch")
                })
                .collect();
            self.core.commit_batches(&write_batches)?;
        }
        self.region.clear();
        self.seeded.clear();
        self.core.checkpoint_manifest()
    }
}
