use super::*;


/// The per-key cleanup deadlines of a deadline-retention operator (the temporal join, the
/// event-time OVER): PK `[kg, k]` (`k` = the operator's join/partition BinaryRow key) and one
/// Int64 `cleanup_at` column — the absolute epoch-ms at which Flink's registered cleanup timer
/// would fire and clear the key. Unlike every other shape, the map itself stays RESIDENT in the
/// operator: the hysteresis re-arm is a read-modify-write on every element, so a read-through
/// deadline would put a point read on the push path. This store therefore carries only the write
/// side — the operator's mutations stage as dirty entries (an upsert per re-arm, a tombstone per
/// fired clear) and commit at the barrier — plus the one full scan a restore hydrates from.
pub(crate) struct PaimonDeadlineStore {
    core: PaimonTableCore,
    /// The write buffer: every deadline written since the last barrier — `Some` an armed
    /// deadline, `None` a cleared key's tombstone. Staging is infallible so the operators'
    /// shared memory-mode retention functions can write through without growing a Result.
    dirty: ahash::HashMap<ByteKey, Option<i64>>,
}

impl PaimonDeadlineStore {
    const ENTRY_BYTES: usize =
        std::mem::size_of::<(ByteKey, Option<i64>)>() + GROUP_ENTRY_OVERHEAD;

    pub(crate) fn create(config: PaimonStoreConfig) -> Result<Self, DataFusionError> {
        let schema = PaimonTableCore::schema_builder(&config)?
            .column(DEADLINE_COLUMN, PaimonType::BigInt(BigIntType::new()))
            .primary_key([KG_COLUMN, KEY_COLUMN])
            .build()
            .map_err(pe)?;
        Ok(PaimonDeadlineStore {
            core: PaimonTableCore::create(config, schema)?,
            dirty: ahash::HashMap::default(),
        })
    }

    pub(crate) fn open_merged(
        config: PaimonStoreConfig,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        let mut store = Self::create(config)?;
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
        vec![
            Field::new(KG_COLUMN, DataType::Int32, false),
            Field::new(KEY_COLUMN, DataType::Binary, false),
            Field::new(DEADLINE_COLUMN, DataType::Int64, true),
        ]
    }

    /// Every committed (key, deadline) — the restore-time hydration of the operator's resident
    /// map, run before anything stages (the dirty buffer is empty, so committed IS current).
    pub(crate) fn hydrate_all(&mut self) -> Result<Vec<(ByteKey, i64)>, DataFusionError> {
        let expected = self.arrow_fields();
        let committed = {
            let builder = PredicateBuilder::new(&self.core.fields);
            let predicate = builder.greater_or_equal(KG_COLUMN, Datum::Int(0)).map_err(pe)?;
            self.core.scan_predicate(predicate)?
        };
        let mut out = Vec::new();
        for batch in committed {
            let keys = normalized_column(&batch, 1, &expected[1])?;
            let keys = keys
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| DataFusionError::Internal("paimon deadline key column".into()))?;
            let deadlines = normalized_column(&batch, 2, &expected[2])?;
            let deadlines = deadlines
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("paimon deadline column".into()))?;
            for row in 0..batch.num_rows() {
                out.push((ByteKey::from(keys.value(row)), deadlines.value(row)));
            }
        }
        Ok(out)
    }

    /// Stages the key's (re-)armed deadline; committed at the barrier.
    pub(crate) fn stage(&mut self, key: &[u8], cleanup_at: i64) {
        self.dirty.insert(ByteKey::from(key), Some(cleanup_at));
    }

    /// Stages the key's tombstone — the fired cleanup clearing the key.
    pub(crate) fn stage_delete(&mut self, key: &[u8]) {
        self.dirty.insert(ByteKey::from(key), None);
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        self.dirty.keys().map(|key| key.0.len() + Self::ENTRY_BYTES).sum()
    }

    /// Checkpoint sync phase: commits the dirty entries (upserts and tombstones) as this table's
    /// snapshot.
    pub(crate) fn checkpoint(&mut self) -> Result<PaimonCheckpointManifest, DataFusionError> {
        self.core.refresh_to_latest()?;
        if !self.dirty.is_empty() {
            let mut kgs: Vec<i32> = Vec::with_capacity(self.dirty.len());
            let mut keys: Vec<&[u8]> = Vec::with_capacity(self.dirty.len());
            let mut deadlines: Vec<Option<i64>> = Vec::with_capacity(self.dirty.len());
            let mut kinds: Vec<i8> = Vec::with_capacity(self.dirty.len());
            for (key, deadline) in &self.dirty {
                kgs.push(self.core.key_group(&key.0));
                keys.push(&key.0);
                deadlines.push(*deadline);
                kinds.push(if deadline.is_some() { 0 } else { 3 });
            }
            let mut fields = self.arrow_fields();
            fields.push(Field::new(VALUE_KIND_COLUMN, DataType::Int8, false));
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int32Array::from(kgs)),
                Arc::new(BinaryArray::from_iter_values(keys)),
                Arc::new(Int64Array::from(deadlines)),
                Arc::new(Int8Array::from(kinds)),
            ];
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("paimon deadline write batch");
            self.core.commit(&batch)?;
        }
        self.dirty.clear();
        self.core.checkpoint_manifest()
    }
}
