use super::*;


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
