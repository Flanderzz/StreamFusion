use super::*;

/// Time-buffered pending rows plus point-access fold state for the event-time OVER aggregate —
/// the third range-read consumer. Two tables live under the operator's state directory:
///
/// * `pending/` — a [`PaimonRowBufferStore`] of the buffered input rows (`rt` = rowtime millis,
///   `kg` routed by the PARTITION BY key). A watermark firing returns the completed rows in
///   arrival order and stages their deletions.
/// * `folds/` — the per-key running state under PK `[kg, k]` (`k` = partition-key BinaryRow),
///   one typed column per running value — the same scalars the memory path's raw snapshot
///   round-trips. Point-access only: reads are the per-batch key probe, writes buffer as dirty
///   slots until the barrier. Unlike the memory path's forever-growing per-key map, the fold
///   rows are disk-resident between firings.
///
/// With idle-state retention on, a third table (`deadlines/`) persists the per-key cleanup
/// deadlines the operator keeps resident.
pub(crate) struct PaimonOverStore {
    pending: PaimonRowBufferStore,
    folds: PaimonTableCore,
    deadlines: Option<PaimonDeadlineStore>,
    state_fields: Vec<Field>,
    fold_working: ahash::HashMap<ByteKey, FoldSlot>,
    last_footprint: usize,
}

/// One fold-state working entry: `dirty` rows are the folds write buffer (pinned until the
/// barrier commit); clean rows are this bundle's committed probes and drop at `end_bundle`.
/// `None` records a probed-absent key — or, dirty, a removed fold the barrier commits as a
/// tombstone (the fired cleanup deadline clearing the key).
struct FoldSlot {
    scalars: Option<Vec<ScalarValue>>,
    dirty: bool,
}

impl PaimonOverStore {
    const FOLD_ENTRY_BYTES: usize =
        std::mem::size_of::<(ByteKey, FoldSlot)>() + GROUP_ENTRY_OVERHEAD;

    pub(super) fn side_config(config: &PaimonStoreConfig, side: &str) -> PaimonStoreConfig {
        PaimonStoreConfig {
            table_dir: format!("{}/{side}", config.table_dir),
            max_parallelism: config.max_parallelism,
            buckets: config.buckets,
            file_format: config.file_format.clone(),
            file_compression: config.file_compression.clone(),
            deletion_vectors: config.deletion_vectors,
            ttl_ms: config.ttl_ms,
        }
    }

    pub(crate) fn create(
        config: PaimonStoreConfig,
        payload_types: Vec<DataType>,
        state_types: Vec<DataType>,
        retention: bool,
    ) -> Result<Self, DataFusionError> {
        let state_fields = PaimonRowBufferStore::typed_fields("s", &state_types)?;
        let pending =
            PaimonRowBufferStore::create(Self::side_config(&config, "pending"), payload_types)?;
        let deadlines = retention
            .then(|| PaimonDeadlineStore::create(Self::side_config(&config, "deadlines")))
            .transpose()?;
        Ok(PaimonOverStore {
            pending,
            folds: Self::create_folds(&config, &state_fields)?,
            deadlines,
            state_fields,
            fold_working: ahash::HashMap::default(),
            last_footprint: 0,
        })
    }

    fn create_folds(
        config: &PaimonStoreConfig,
        state_fields: &[Field],
    ) -> Result<PaimonTableCore, DataFusionError> {
        let folds_config = Self::side_config(config, "folds");
        let mut builder = PaimonTableCore::schema_builder(&folds_config)?;
        for field in state_fields {
            let paimon_type = paimon_type_of(field.data_type()).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "state type {} not supported by the paimon state backend",
                    field.data_type()
                ))
            })?;
            builder = builder.column(field.name(), paimon_type);
        }
        let folds_schema = builder.primary_key([KG_COLUMN, KEY_COLUMN]).build().map_err(pe)?;
        PaimonTableCore::create(folds_config, folds_schema)
    }

    /// Restores from checkpoint sources, each side independently: a source that never committed
    /// a side (snapshot id `-1`) is skipped for it. Aligned single-source restores adopt files
    /// wholesale; anything else clips by key-group range.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_merged(
        config: PaimonStoreConfig,
        payload_types: Vec<DataType>,
        state_types: Vec<DataType>,
        pending_sources: &[(String, i64)],
        fold_sources: &[(String, i64)],
        deadline_sources: &[(String, i64)],
        retention: bool,
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        let state_fields = PaimonRowBufferStore::typed_fields("s", &state_types)?;
        let pending = PaimonRowBufferStore::open_merged(
            Self::side_config(&config, "pending"),
            payload_types,
            pending_sources,
            key_groups.clone(),
            aligned,
        )?;
        let deadlines = retention
            .then(|| {
                PaimonDeadlineStore::open_merged(
                    Self::side_config(&config, "deadlines"),
                    deadline_sources,
                    key_groups.clone(),
                    aligned,
                )
            })
            .transpose()?;
        let mut store = PaimonOverStore {
            pending,
            folds: Self::create_folds(&config, &state_fields)?,
            deadlines,
            state_fields,
            fold_working: ahash::HashMap::default(),
            last_footprint: 0,
        };
        if !fold_sources.is_empty() {
            let adopted = aligned
                && fold_sources.len() == 1
                && store.folds.adopt_all(&fold_sources[0].0, fold_sources[0].1)?;
            if !adopted {
                let fold_fields = store.folds_arrow_fields();
                store.folds.clip_from_sources(fold_sources, key_groups, &fold_fields, crate::state::StateTtl::disabled())?;
            }
        }
        Ok(store)
    }

    pub(crate) fn deadlines_mut(&mut self) -> &mut PaimonDeadlineStore {
        self.deadlines.as_mut().expect("over deadlines table")
    }

    /// The folds table's persisted row schema (also its clip write schema).
    fn folds_arrow_fields(&self) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KG_COLUMN, DataType::Int32, false),
            Field::new(KEY_COLUMN, DataType::Binary, false),
        ];
        fields.extend(self.state_fields.iter().cloned());
        fields
    }

    pub(crate) fn key_group(&self, key: &[u8]) -> i32 {
        self.pending.key_group(key)
    }

    pub(crate) fn next_seq(&self) -> i64 {
        self.pending.next_seq()
    }

    pub(crate) fn set_next_seq(&mut self, seq: i64) {
        self.pending.set_next_seq(seq);
    }

    /// Buffers one input batch's rows as pending state — see [`PaimonRowBufferStore::stage`].
    pub(crate) fn stage_pending(
        &mut self,
        kgs: &[i32],
        rts: Vec<i64>,
        payload: Vec<ArrayRef>,
    ) -> Result<(), DataFusionError> {
        self.pending.stage(kgs, rts, payload)
    }

    /// Every pending row the watermark completed — see [`PaimonRowBufferStore::fire`].
    pub(crate) fn fire(
        &mut self,
        watermark: i64,
        ctx: Arc<TaskContext>,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        self.pending.fire(watermark, ctx)
    }

    /// Fetches the committed fold state for every given key this bundle doesn't already hold —
    /// one point-read join per firing, recording absent keys too.
    pub(crate) fn ensure_folds(&mut self, keys: &[ByteKey]) -> Result<(), DataFusionError> {
        let misses: Vec<ByteKey> = keys
            .iter()
            .filter(|k| !self.fold_working.contains_key(&*k.0))
            .cloned()
            .collect();
        if misses.is_empty() {
            return Ok(());
        }
        let expected = self.folds_arrow_fields();
        for batch in self.folds.scan_keys(&misses)? {
            let ks = normalized_column(&batch, 1, &expected[1])?;
            let ks = ks
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| DataFusionError::Internal("paimon fold key column".into()))?;
            for row in 0..batch.num_rows() {
                let mut scalars = Vec::with_capacity(self.state_fields.len());
                for (i, field) in expected.iter().enumerate().skip(2) {
                    let column = normalized_column(&batch, i, field)?;
                    scalars.push(
                        ScalarValue::try_from_array(&column, row)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?,
                    );
                }
                self.fold_working.insert(
                    ByteKey::from(ks.value(row)),
                    FoldSlot { scalars: Some(scalars), dirty: false },
                );
            }
        }
        for key in misses {
            self.fold_working.entry(key).or_insert(FoldSlot { scalars: None, dirty: false });
        }
        Ok(())
    }

    /// The key's running state, if the store holds one. Callers must have run `ensure_folds` for
    /// the key this bundle.
    pub(crate) fn fold_scalars(&self, key: &[u8]) -> Option<&[ScalarValue]> {
        self.fold_working.get(key).and_then(|slot| slot.scalars.as_deref())
    }

    /// Writes a key's updated running state into the write buffer (committed at the barrier).
    pub(crate) fn put_fold(&mut self, key: &[u8], scalars: Vec<ScalarValue>) {
        self.fold_working
            .insert(ByteKey::from(key), FoldSlot { scalars: Some(scalars), dirty: true });
    }

    /// Removes a key's running state — the fired cleanup deadline clearing the key. The dirty
    /// absent slot survives the bundle and commits as a `-D` tombstone at the barrier, so a
    /// restore cannot resurrect the cleared fold.
    pub(crate) fn remove_fold(&mut self, key: &[u8]) {
        self.fold_working.insert(ByteKey::from(key), FoldSlot { scalars: None, dirty: true });
    }

    /// The committed folds table's keys — restore-time only (the write buffer is empty), the
    /// deadline retention's enable-flip stamp scan.
    pub(crate) fn scan_fold_keys(&mut self) -> Result<Vec<ByteKey>, DataFusionError> {
        let expected = self.folds_arrow_fields();
        let committed = {
            let builder = PredicateBuilder::new(&self.folds.fields);
            let predicate = builder.greater_or_equal(KG_COLUMN, Datum::Int(0)).map_err(pe)?;
            self.folds.scan_predicate(predicate)?
        };
        let mut keys = Vec::new();
        for batch in committed {
            let ks = normalized_column(&batch, 1, &expected[1])?;
            let ks = ks
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| DataFusionError::Internal("paimon fold key column".into()))?;
            for row in 0..batch.num_rows() {
                keys.push(ByteKey::from(ks.value(row)));
            }
        }
        Ok(keys)
    }

    /// The committed pending rows' payload columns (the buffered input rows) — restore-time
    /// only, deriving the deadline retention's per-key deferral counts: the pending PK is the
    /// arrival sequence, so only the payload's PARTITION BY columns identify the key.
    pub(crate) fn scan_pending_payload(&mut self) -> Result<Vec<RecordBatch>, DataFusionError> {
        let mut out = Vec::new();
        for batch in self.pending.scan_all()? {
            let fields: Vec<Field> =
                batch.schema().fields()[3..].iter().map(|f| f.as_ref().clone()).collect();
            out.push(
                RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns()[3..].to_vec())
                    .expect("pending payload projection"),
            );
        }
        Ok(out)
    }

    /// End of the operator's bundle: clean fold probes drop; the dirty slots (the folds write
    /// buffer) and the pending region survive to the barrier.
    pub(crate) fn end_bundle(&mut self) {
        self.fold_working.retain(|_, slot| slot.dirty);
    }

    /// The store's untracked footprint change since the last call.
    pub(crate) fn footprint_delta(&mut self) -> isize {
        let current = self.pending.heap_bytes()
            + self.deadlines.as_ref().map(PaimonDeadlineStore::heap_bytes).unwrap_or(0)
            + self
                .fold_working
                .iter()
                .map(|(k, slot)| {
                    k.0.len()
                        + Self::FOLD_ENTRY_BYTES
                        + slot
                            .scalars
                            .as_ref()
                            .map(|s| scalar_row_bytes(s))
                            .unwrap_or(0)
                })
                .sum::<usize>();
        let delta = current as isize - self.last_footprint as isize;
        self.last_footprint = current;
        delta
    }

    /// Checkpoint sync phase, called at the barrier: commits the pending region (live rows and
    /// fired deletions), the dirty fold rows — an upsert per updated fold, a `-D` per removed
    /// one — and the staged deadlines as each table's snapshot. Returns the manifests (the
    /// deadlines manifest `absent` while retention is off); the caller packs them plus the
    /// arrival sequence into the snapshot token.
    #[allow(clippy::type_complexity)]
    pub(crate) fn checkpoint(
        &mut self,
    ) -> Result<
        (PaimonCheckpointManifest, PaimonCheckpointManifest, PaimonCheckpointManifest),
        DataFusionError,
    > {
        let pending_manifest = self.pending.checkpoint()?;

        self.folds.refresh_to_latest()?;
        let dirty: Vec<(&ByteKey, &FoldSlot)> =
            self.fold_working.iter().filter(|(_, slot)| slot.dirty).collect();
        if !dirty.is_empty() {
            let mut fields = self.folds_arrow_fields();
            fields.push(Field::new(VALUE_KIND_COLUMN, DataType::Int8, false));
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());
            columns.push(Arc::new(Int32Array::from(
                dirty.iter().map(|(k, _)| self.folds.key_group(&k.0)).collect::<Vec<_>>(),
            )));
            columns.push(Arc::new(BinaryArray::from_iter_values(
                dirty.iter().map(|(k, _)| k.0.as_ref()),
            )));
            for (j, field) in self.state_fields.iter().enumerate() {
                columns.push(scalars_to_array(
                    dirty
                        .iter()
                        .map(|(_, slot)| match &slot.scalars {
                            Some(scalars) => scalars[j].clone(),
                            None => null_scalar(field.data_type()),
                        })
                        .collect(),
                    field.data_type(),
                ));
            }
            columns.push(Arc::new(Int8Array::from(
                dirty
                    .iter()
                    .map(|(_, slot)| if slot.scalars.is_some() { 0i8 } else { 3i8 })
                    .collect::<Vec<_>>(),
            )));
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("paimon folds write batch");
            self.folds.commit(&batch)?;
        }
        self.fold_working.clear();
        let deadlines_manifest = match &mut self.deadlines {
            Some(store) => store.checkpoint()?,
            None => PaimonCheckpointManifest::absent(),
        };
        Ok((pending_manifest, self.folds.checkpoint_manifest()?, deadlines_manifest))
    }
}
