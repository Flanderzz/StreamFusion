use super::*;


/// The versioned build side of a temporal join — the eighth range-read consumer's new half. One
/// table row per (key, version) under PK `[kg, k, rt]` (`rt` = the version's right rowtime):
/// Flink's `rightState.put(rowTime, row)` last-write-wins-per-timestamp is literally the
/// deduplicate merge engine, so writes are plain upserts and every changelog `RowKind` persists
/// as a column (a retract version marks "no row here", exactly as in memory). Reads are the
/// per-advance key probe: the fired probe rows' keys pull their version sets (committed rows
/// minus region-superseded, plus the region's live rows) and the operator rebuilds each key's
/// ordered map. Version pruning is *lazy*, a deliberate deviation from the memory path (which
/// prunes every key at every watermark, cheap in RAM): a probed key prunes its stale versions
/// (all below the latest one at or under the watermark) via staged deletions; an unprobed key's
/// old versions sit on disk until its next probe.
pub(crate) struct PaimonTemporalRightStore {
    core: PaimonTableCore,
    payload_fields: Vec<Field>,
    region: DirtyRegion,
}

impl PaimonTemporalRightStore {
    fn value_fields(payload_fields: &[Field]) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KEY_COLUMN, DataType::Binary, false),
            Field::new(RT_COLUMN, DataType::Int64, false),
            Field::new(KIND_COLUMN, DataType::Int8, true),
        ];
        fields.extend(payload_fields.iter().cloned());
        fields
    }

    pub(crate) fn create(
        config: PaimonStoreConfig,
        payload_types: Vec<DataType>,
    ) -> Result<Self, DataFusionError> {
        let payload_fields = PaimonRowBufferStore::typed_fields("c", &payload_types)?;
        let mut builder = PaimonTableCore::schema_builder(&config)?
            .column(RT_COLUMN, PaimonType::BigInt(BigIntType::new()))
            .column(KIND_COLUMN, PaimonType::TinyInt(TinyIntType::new()));
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
            .primary_key([KG_COLUMN, KEY_COLUMN, RT_COLUMN])
            .build()
            .map_err(pe)?;
        let region = DirtyRegion::new(Self::value_fields(&payload_fields), Some(1));
        Ok(PaimonTemporalRightStore {
            core: PaimonTableCore::create(config, schema)?,
            payload_fields,
            region,
        })
    }

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

    /// The persisted row schema (also the clip write schema).
    fn arrow_fields(&self) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KG_COLUMN, DataType::Int32, false),
            Field::new(KEY_COLUMN, DataType::Binary, false),
        ];
        fields.extend(Self::value_fields(&self.payload_fields)[1..].to_vec());
        fields
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        self.region.heap_bytes()
    }

    fn composite_key(key: &[u8], rt: i64) -> Vec<u8> {
        let mut composite = Vec::with_capacity(key.len() + 8);
        composite.extend_from_slice(key);
        composite.extend_from_slice(&rt.to_be_bytes());
        composite
    }

    /// Folds one build-side batch: an upsert per (key, version) — same-timestamp rewrites are
    /// last-write-wins through the region within an interval and through the merge engine across
    /// barriers.
    pub(crate) fn stage(
        &mut self,
        keys: &[&[u8]],
        rts: Vec<i64>,
        kinds: Vec<i8>,
        payload: Vec<ArrayRef>,
    ) -> Result<(), DataFusionError> {
        if keys.is_empty() {
            return Ok(());
        }
        let composites: Vec<Vec<u8>> = keys
            .iter()
            .zip(&rts)
            .map(|(key, &rt)| Self::composite_key(key, rt))
            .collect();
        let composite_slices: Vec<&[u8]> = composites.iter().map(|k| k.as_slice()).collect();
        let key_groups: Vec<i32> = keys.iter().map(|key| self.core.key_group(key)).collect();
        let mut values: Vec<ArrayRef> = Vec::with_capacity(3 + payload.len());
        values.push(Arc::new(BinaryArray::from_iter_values(keys)));
        values.push(Arc::new(Int64Array::from(rts)));
        values.push(Arc::new(Int8Array::from(kinds)));
        values.extend(payload);
        self.region.append_upserts(&composite_slices, &key_groups, values)
    }

    /// The version sets of the given keys — committed rows minus region-superseded, plus the
    /// region's live rows — as store-schema batches (`kg`, `k`, `rt`, `kind`, payload…). The
    /// caller rebuilds each key's ordered map; row order here is irrelevant.
    pub(crate) fn probe(
        &mut self,
        keys: &[ByteKey],
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let mut out: Vec<RecordBatch> = Vec::new();
        let expected = self.arrow_fields();
        for batch in self.core.scan_keys(keys)? {
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(expected.len());
            for (i, field) in expected.iter().enumerate() {
                columns.push(normalized_column(&batch, i, field)?);
            }
            let ks = columns[1].as_any().downcast_ref::<BinaryArray>().expect("k column");
            let rts = columns[2].as_any().downcast_ref::<Int64Array>().expect("rt column");
            let mask: BooleanArray = (0..ks.len())
                .map(|row| {
                    Some(!self.region.contains(&Self::composite_key(
                        ks.value(row),
                        rts.value(row),
                    )))
                })
                .collect();
            let normalized =
                RecordBatch::try_new(Arc::new(Schema::new(expected.clone())), columns)
                    .expect("temporal probe batch");
            let filtered = filter_record_batch(&normalized, &mask)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
        }
        let wanted: StdHashSet<&[u8]> = keys.iter().map(|k| k.0.as_ref()).collect();
        for batch in self.region.live_upserts(None)? {
            let ks = batch
                .column(2)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("region k column");
            let mask: BooleanArray = (0..batch.num_rows())
                .map(|row| Some(wanted.contains(ks.value(row))))
                .collect();
            let filtered = filter_record_batch(&batch, &mask)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            if filtered.num_rows() > 0 {
                let mut columns: Vec<ArrayRef> = Vec::with_capacity(filtered.num_columns() - 1);
                columns.push(filtered.column(0).clone());
                columns.extend(filtered.columns()[2..].iter().cloned());
                out.push(
                    RecordBatch::try_new(Arc::new(Schema::new(self.arrow_fields())), columns)
                        .expect("region temporal probe batch"),
                );
            }
        }
        Ok(out)
    }

    /// Stages a `-D` per (key, version) — the lazy prune of a probed key's stale versions.
    pub(crate) fn stage_deletes(
        &mut self,
        keys: &[&[u8]],
        rts: &[i64],
    ) -> Result<(), DataFusionError> {
        if keys.is_empty() {
            return Ok(());
        }
        let composites: Vec<Vec<u8>> = keys
            .iter()
            .zip(rts)
            .map(|(key, &rt)| Self::composite_key(key, rt))
            .collect();
        let composite_slices: Vec<&[u8]> = composites.iter().map(|k| k.as_slice()).collect();
        let key_groups: Vec<i32> = keys.iter().map(|key| self.core.key_group(key)).collect();
        let mut values: Vec<ArrayRef> = vec![
            Arc::new(BinaryArray::from_iter_values(keys)),
            Arc::new(Int64Array::from(rts.to_vec())),
            new_null_array(&DataType::Int8, keys.len()),
        ];
        for field in &self.payload_fields {
            values.push(new_null_array(field.data_type(), keys.len()));
        }
        self.region.append_deletes(&composite_slices, &key_groups, values)
    }

    /// Checkpoint sync phase: commits the region as this table's snapshot.
    pub(crate) fn checkpoint(&mut self) -> Result<PaimonCheckpointManifest, DataFusionError> {
        self.core.refresh_to_latest()?;
        let flushed = self.region.flush_batches()?;
        if !flushed.is_empty() {
            let mut fields = self.arrow_fields();
            fields.push(Field::new(VALUE_KIND_COLUMN, DataType::Int8, false));
            let write_schema = Arc::new(Schema::new(fields));
            let write_batches: Vec<RecordBatch> = flushed
                .iter()
                .map(|batch| {
                    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns() - 1);
                    columns.push(batch.column(0).clone());
                    columns.extend(batch.columns()[2..].iter().cloned());
                    RecordBatch::try_new(write_schema.clone(), columns)
                        .expect("temporal-right write batch")
                })
                .collect();
            self.core.commit_batches(&write_batches)?;
        }
        self.region.clear();
        self.core.checkpoint_manifest()
    }
}

/// Both sides of a temporal join under one operator directory (`left/`, `right/`): the probe
/// side is a keyed row buffer (the interval-join side store, its matched flag unused, the
/// changelog kind packed as a trailing payload column) and the build side the versioned store
/// above. The snapshot token packs both snapshot ids and the probe side's arrival sequence.
pub(crate) struct PaimonTemporalJoinStore {
    pub(crate) left: PaimonIntervalSideStore,
    pub(crate) right: PaimonTemporalRightStore,
    last_footprint: usize,
}

impl PaimonTemporalJoinStore {
    pub(crate) fn create(
        config: PaimonStoreConfig,
        left_types: Vec<DataType>,
        right_types: Vec<DataType>,
    ) -> Result<Self, DataFusionError> {
        Ok(PaimonTemporalJoinStore {
            left: PaimonIntervalSideStore::create(
                PaimonOverStore::side_config(&config, "left"),
                left_types,
            )?,
            right: PaimonTemporalRightStore::create(
                PaimonOverStore::side_config(&config, "right"),
                right_types,
            )?,
            last_footprint: 0,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_merged(
        config: PaimonStoreConfig,
        left_types: Vec<DataType>,
        right_types: Vec<DataType>,
        left_sources: &[(String, i64)],
        right_sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        Ok(PaimonTemporalJoinStore {
            left: PaimonIntervalSideStore::open_merged(
                PaimonOverStore::side_config(&config, "left"),
                left_types,
                left_sources,
                key_groups.clone(),
                aligned,
            )?,
            right: PaimonTemporalRightStore::open_merged(
                PaimonOverStore::side_config(&config, "right"),
                right_types,
                right_sources,
                key_groups,
                aligned,
            )?,
            last_footprint: 0,
        })
    }

    /// The store's untracked footprint change since the last call.
    pub(crate) fn footprint_delta(&mut self) -> isize {
        let current = self.left.heap_bytes() + self.right.heap_bytes();
        let delta = current as isize - self.last_footprint as isize;
        self.last_footprint = current;
        delta
    }

    /// Checkpoint sync phase: commits both sides; the caller packs the two manifests and the
    /// probe side's arrival sequence into the snapshot token.
    pub(crate) fn checkpoint(
        &mut self,
    ) -> Result<(PaimonCheckpointManifest, PaimonCheckpointManifest), DataFusionError> {
        Ok((self.left.checkpoint()?, self.right.checkpoint()?))
    }
}
