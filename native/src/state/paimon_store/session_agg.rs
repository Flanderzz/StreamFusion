use super::*;

/// Keyed open-session state for the session-window aggregate — the sixth range-read consumer.
/// One table row per open (key, session) under PK `[kg, k, ws]`: `ws` is the session start; the
/// end is a *value* column, because a session extends by growing its end under the same start,
/// and a merge removes starts (tombstones at the barrier). Otherwise the aligned-window-aggregate
/// discipline: the interval's touched keys' sessions live decoded in the operator (seeded on a
/// key's first touch — recording which committed starts were loaded, so the barrier can tombstone
/// the ones a merge consumed), stage wholesale at the barrier, and a firing hydrates committed
/// sessions under `we ≤ watermark` into the same decoded map, skipping seeded keys outright (a
/// seeded key's map is authoritative for the whole key: a merge may have consumed a committed
/// start, and matching per (key, start) would resurrect it). The memory path persists no
/// watermark, so the snapshot token is the plain snapshot id.
pub(crate) struct PaimonSessionAggStore {
    core: PaimonTableCore,
    key_fields: Vec<Field>,
    state_fields: Vec<Field>,
    region: DirtyRegion,
    /// Keys seeded this interval, with the committed starts their seed scan returned — the
    /// barrier tombstones the loaded starts that no live session carries anymore.
    seeded: ahash::HashMap<ByteKey, Vec<i64>>,
    last_footprint: usize,
}

impl PaimonSessionAggStore {
    const SEEDED_ENTRY_BYTES: usize =
        std::mem::size_of::<(ByteKey, Vec<i64>)>() + GROUP_ENTRY_OVERHEAD;

    /// The region's value columns: the table's PK components after `kg`, then the session end,
    /// the typed key columns, and the accumulator state fields.
    fn value_fields(key_fields: &[Field], state_fields: &[Field]) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KEY_COLUMN, DataType::Binary, false),
            Field::new(WINDOW_START_COLUMN, DataType::Int64, false),
            Field::new(WINDOW_END_COLUMN, DataType::Int64, true),
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
            .column(WINDOW_START_COLUMN, PaimonType::BigInt(BigIntType::new()))
            .column(WINDOW_END_COLUMN, PaimonType::BigInt(BigIntType::new()));
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
            .primary_key([KG_COLUMN, KEY_COLUMN, WINDOW_START_COLUMN])
            .build()
            .map_err(pe)?;
        let region = DirtyRegion::new(Self::value_fields(&key_fields, &state_fields), Some(2));
        Ok(PaimonSessionAggStore {
            core: PaimonTableCore::create(config, schema)?,
            key_fields,
            state_fields,
            region,
            seeded: ahash::HashMap::default(),
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

    fn composite_key(key: &[u8], ws: i64) -> Vec<u8> {
        let mut composite = Vec::with_capacity(key.len() + 8);
        composite.extend_from_slice(key);
        composite.extend_from_slice(&ws.to_be_bytes());
        composite
    }

    /// The committed sessions of every given key not yet seeded this interval — normalized
    /// store-schema batches (`kg`, `k`, `ws`, `we`, keys…, states…), minus rows the region
    /// already deleted (fired earlier this interval). Records each key's loaded starts for the
    /// barrier diff.
    pub(crate) fn seed_scan(
        &mut self,
        keys: &[ByteKey],
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let misses: Vec<ByteKey> =
            keys.iter().filter(|k| !self.seeded.contains_key(*k)).cloned().collect();
        if misses.is_empty() {
            return Ok(Vec::new());
        }
        for key in &misses {
            self.seeded.insert(key.clone(), Vec::new());
        }
        let batches = self.core.scan_keys(&misses)?;
        let normalized = self.normalize(batches)?;
        let filtered = self.filter_region_deleted(normalized, None, true)?;
        for batch in &filtered {
            let ks = batch.column(1).as_any().downcast_ref::<BinaryArray>().expect("k column");
            let wss = batch.column(2).as_any().downcast_ref::<Int64Array>().expect("ws column");
            for row in 0..batch.num_rows() {
                self.seeded
                    .get_mut(ks.value(row))
                    .expect("seeded key recorded")
                    .push(wss.value(row));
            }
        }
        Ok(filtered)
    }

    /// The committed sessions every key the watermark closes — minus region-deleted rows and
    /// minus rows of seeded keys (their decoded maps are authoritative for the whole key).
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
        self.filter_region_deleted(normalized, Some(watermark), false)
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
                RecordBatch::try_new(schema.clone(), columns)
                    .expect("session-agg normalized batch"),
            );
        }
        Ok(out)
    }

    /// Drops region-deleted rows; re-checks the range predicate when a watermark is given; and,
    /// unless seeding, drops rows of seeded keys.
    fn filter_region_deleted(
        &self,
        batches: Vec<RecordBatch>,
        watermark: Option<i64>,
        seeding: bool,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let mut out = Vec::new();
        for batch in batches {
            let ks = batch.column(1).as_any().downcast_ref::<BinaryArray>().expect("k column");
            let wss = batch.column(2).as_any().downcast_ref::<Int64Array>().expect("ws column");
            let wes = batch.column(3).as_any().downcast_ref::<Int64Array>().expect("we column");
            let mask: BooleanArray = (0..batch.num_rows())
                .map(|row| {
                    let key = ks.value(row);
                    Some(
                        watermark.is_none_or(|wm| wes.is_valid(row) && wes.value(row) <= wm)
                            && (seeding || !self.seeded.contains_key(key))
                            && !self
                                .region
                                .contains(&Self::composite_key(key, wss.value(row))),
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

    /// The committed starts a key's seed scan loaded this interval (empty for unseeded keys).
    pub(crate) fn seeded_starts(&self, key: &[u8]) -> &[i64] {
        self.seeded.get(key).map(|starts| starts.as_slice()).unwrap_or(&[])
    }

    /// Stages one row per open (key, session) — the barrier's whole-state rewrite.
    pub(crate) fn stage_upserts(
        &mut self,
        keys: &[&[u8]],
        wss: &[i64],
        wes: &[i64],
        key_columns: Vec<ArrayRef>,
        state_columns: Vec<ArrayRef>,
    ) -> Result<(), DataFusionError> {
        if keys.is_empty() {
            return Ok(());
        }
        let composites: Vec<Vec<u8>> = keys
            .iter()
            .zip(wss)
            .map(|(key, &ws)| Self::composite_key(key, ws))
            .collect();
        let composite_slices: Vec<&[u8]> = composites.iter().map(|k| k.as_slice()).collect();
        let key_groups: Vec<i32> = keys.iter().map(|key| self.core.key_group(key)).collect();
        let mut values: Vec<ArrayRef> =
            Vec::with_capacity(3 + key_columns.len() + state_columns.len());
        values.push(Arc::new(BinaryArray::from_iter_values(keys)));
        values.push(Arc::new(Int64Array::from(wss.to_vec())));
        values.push(Arc::new(Int64Array::from(wes.to_vec())));
        values.extend(key_columns);
        values.extend(state_columns);
        self.region.append_upserts(&composite_slices, &key_groups, values)
    }

    /// Stages a `-D` per (key, start) — fired sessions and starts a merge consumed.
    pub(crate) fn stage_deletes(
        &mut self,
        keys: &[&[u8]],
        wss: &[i64],
    ) -> Result<(), DataFusionError> {
        if keys.is_empty() {
            return Ok(());
        }
        let composites: Vec<Vec<u8>> = keys
            .iter()
            .zip(wss)
            .map(|(key, &ws)| Self::composite_key(key, ws))
            .collect();
        let composite_slices: Vec<&[u8]> = composites.iter().map(|k| k.as_slice()).collect();
        let key_groups: Vec<i32> = keys.iter().map(|key| self.core.key_group(key)).collect();
        let mut values: Vec<ArrayRef> =
            Vec::with_capacity(3 + self.key_fields.len() + self.state_fields.len());
        values.push(Arc::new(BinaryArray::from_iter_values(keys)));
        values.push(Arc::new(Int64Array::from(wss.to_vec())));
        values.push(new_null_array(&DataType::Int64, keys.len()));
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
                .map(|(k, starts)| {
                    k.0.len() + Self::SEEDED_ENTRY_BYTES + starts.len() * 8
                })
                .sum::<usize>();
        let delta = current as isize - self.last_footprint as isize;
        self.last_footprint = current;
        delta
    }

    /// Checkpoint sync phase: commits the region (staged sessions, fired and merged-away
    /// deletions) as the checkpoint's snapshot. The caller stages the open sessions and clears
    /// its decoded map first; the seeded map resets — the next interval re-probes on demand.
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
                        .expect("session-agg write batch")
                })
                .collect();
            self.core.commit_batches(&write_batches)?;
        }
        self.region.clear();
        self.seeded.clear();
        self.core.checkpoint_manifest()
    }
}
