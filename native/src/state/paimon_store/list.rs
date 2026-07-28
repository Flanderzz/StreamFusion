use super::*;


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
