use super::*;


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
