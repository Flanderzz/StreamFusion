use super::*;


/// One side of an interval join — the seventh range-read consumer: whole input rows under PK
/// `[kg, k, seq]` (`k` = the equi-join key's BinaryRow, `seq` = an arrival sequence), with the
/// row's time (millis), a `matched` flag (outer joins only care), and the typed payload. Unlike
/// the plain row buffer, reads happen on *push*: the incoming batch probes the opposite side by
/// its equi keys with overlay semantics — committed rows for those keys, minus rows the region
/// already superseded or deleted, plus the region's live rows for those keys — merged back into
/// arrival order by the sequence. Eviction is the range read (`rt ≤ bound`), returning the
/// evicted rows (an outer side null-pads its never-matched ones) and staging their deletions.
/// A committed row that matches marks itself through the region: its full probe row re-stages
/// with `matched = true` — the keep-first fired-marker pattern.
pub(crate) struct PaimonIntervalSideStore {
    core: PaimonTableCore,
    payload_fields: Vec<Field>,
    region: DirtyRegion,
    next_seq: i64,
}

impl PaimonIntervalSideStore {
    /// The region's value columns: the table's PK components after `kg`, the time, the matched
    /// flag, then the typed payload.
    fn value_fields(payload_fields: &[Field]) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KEY_COLUMN, DataType::Binary, false),
            Field::new(SEQ_COLUMN, DataType::Int64, false),
            Field::new(RT_COLUMN, DataType::Int64, true),
            Field::new(MATCHED_COLUMN, DataType::Boolean, true),
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
            .column(SEQ_COLUMN, PaimonType::BigInt(BigIntType::new()))
            .column(RT_COLUMN, PaimonType::BigInt(BigIntType::new()))
            .column(MATCHED_COLUMN, PaimonType::Boolean(BooleanType::new()));
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
            .primary_key([KG_COLUMN, KEY_COLUMN, SEQ_COLUMN])
            .build()
            .map_err(pe)?;
        let region = DirtyRegion::new(Self::value_fields(&payload_fields), Some(2));
        Ok(PaimonIntervalSideStore {
            core: PaimonTableCore::create(config, schema)?,
            payload_fields,
            region,
            next_seq: 0,
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

    fn composite_key(key: &[u8], seq: i64) -> Vec<u8> {
        let mut composite = Vec::with_capacity(key.len() + 8);
        composite.extend_from_slice(key);
        composite.extend_from_slice(&seq.to_be_bytes());
        composite
    }

    /// Buffers one input batch's rows, assigning arrival sequences, and returns the first
    /// sequence assigned (the caller aligns its join row-ids to them).
    pub(crate) fn stage(
        &mut self,
        keys: &[&[u8]],
        rts: Vec<i64>,
        matched: Vec<bool>,
        payload: Vec<ArrayRef>,
    ) -> Result<i64, DataFusionError> {
        let n = keys.len();
        let first_seq = self.next_seq;
        if n == 0 {
            return Ok(first_seq);
        }
        let seqs: Vec<i64> = (0..n as i64).map(|i| first_seq + i).collect();
        self.next_seq += n as i64;
        let composites: Vec<Vec<u8>> = keys
            .iter()
            .zip(&seqs)
            .map(|(key, &seq)| Self::composite_key(key, seq))
            .collect();
        let composite_slices: Vec<&[u8]> = composites.iter().map(|k| k.as_slice()).collect();
        let key_groups: Vec<i32> = keys.iter().map(|key| self.core.key_group(key)).collect();
        let mut values: Vec<ArrayRef> = Vec::with_capacity(4 + payload.len());
        values.push(Arc::new(BinaryArray::from_iter_values(keys)));
        values.push(Arc::new(Int64Array::from(seqs)));
        values.push(Arc::new(Int64Array::from(rts)));
        values.push(Arc::new(arrow::array::BooleanArray::from(matched)));
        values.extend(payload);
        self.region.append_upserts(&composite_slices, &key_groups, values)?;
        Ok(first_seq)
    }

    /// This side's live rows for the given equi keys — the opposite side's push probe. Committed
    /// rows for the keys, minus rows the region superseded or deleted, plus the region's live
    /// rows for the keys, merged into arrival order. Store schema (`kg`, `k`, `seq`, `rt`,
    /// `matched`, payload…), `None` when nothing matches.
    pub(crate) fn probe(
        &mut self,
        keys: &[ByteKey],
        ctx: Arc<TaskContext>,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        let mut rows: Vec<RecordBatch> = Vec::new();
        let committed = self.core.scan_keys(keys)?;
        if !committed.is_empty() {
            if self.region.is_empty() {
                rows = committed;
            } else {
                let touched: Vec<&[u8]> =
                    self.region.touched_keys().map(|k| k.0.as_ref()).collect();
                let keys_batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("k", DataType::Binary, false)])),
                    vec![Arc::new(BinaryArray::from_iter_values(touched))],
                )
                .expect("anti-join key batch");
                // The committed rows' composite (k ++ seq) addresses them in the region.
                let expected = self.arrow_fields();
                let mut recomposed: Vec<RecordBatch> = Vec::with_capacity(committed.len());
                for batch in committed {
                    let ks = normalized_column(&batch, 1, &expected[1])?;
                    let ks = ks
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or_else(|| DataFusionError::Internal("paimon k column".into()))?;
                    let seqs = normalized_column(&batch, 2, &expected[2])?;
                    let seqs = seqs
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| DataFusionError::Internal("paimon seq column".into()))?;
                    let composites: Vec<Vec<u8>> = (0..batch.num_rows())
                        .map(|row| Self::composite_key(ks.value(row), seqs.value(row)))
                        .collect();
                    let mut columns = batch.columns().to_vec();
                    columns.push(Arc::new(BinaryArray::from_iter_values(
                        composites.iter().map(|c| c.as_slice()),
                    )));
                    let mut fields: Vec<Field> =
                        batch.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
                    fields.push(Field::new("__composite__", DataType::Binary, false));
                    recomposed.push(
                        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                            .expect("composite-keyed probe batch"),
                    );
                }
                let composite_index = recomposed[0].num_columns() - 1;
                let survivors = crate::join_common::hash_join_right_anti(
                    keys_batch,
                    recomposed,
                    &[(0, composite_index)],
                    ctx,
                )?;
                for batch in survivors {
                    // Drop the trailing composite column again.
                    let fields: Vec<Field> = batch.schema().fields()[..composite_index]
                        .iter()
                        .map(|f| f.as_ref().clone())
                        .collect();
                    rows.push(
                        RecordBatch::try_new(
                            Arc::new(Schema::new(fields)),
                            batch.columns()[..composite_index].to_vec(),
                        )
                        .expect("probe batch reprojection"),
                    );
                }
            }
        }
        // The region's live rows for the probed keys.
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
                // Region schema is [kg, composite, k, seq, rt, matched, payload…]; drop the
                // composite so the columns line up with the store schema.
                let mut columns: Vec<ArrayRef> = Vec::with_capacity(filtered.num_columns() - 1);
                columns.push(filtered.column(0).clone());
                columns.extend(filtered.columns()[2..].iter().cloned());
                rows.push(
                    RecordBatch::try_new(
                        Arc::new(Schema::new(self.arrow_fields())),
                        columns,
                    )
                    .expect("region probe batch"),
                );
            }
        }
        self.merge_by_seq(rows)
    }

    /// The rows the watermark evicts (`rt ≤ bound`) — committed minus region-deleted, plus the
    /// region's live rows in range — in arrival order, with their deletions staged. An outer
    /// side null-pads the returned rows whose `matched` is false.
    pub(crate) fn evict(
        &mut self,
        bound: i64,
        ctx: Arc<TaskContext>,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        let mut rows: Vec<RecordBatch> = Vec::new();
        let committed = {
            let builder = PredicateBuilder::new(&self.core.fields);
            let predicate =
                builder.less_or_equal(RT_COLUMN, Datum::Long(bound)).map_err(pe)?;
            self.core.scan_predicate(predicate)?
        };
        let expected = self.arrow_fields();
        for batch in committed {
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(expected.len());
            for (i, field) in expected.iter().enumerate() {
                columns.push(normalized_column(&batch, i, field)?);
            }
            let ks = columns[1].as_any().downcast_ref::<BinaryArray>().expect("k column");
            let seqs = columns[2].as_any().downcast_ref::<Int64Array>().expect("seq column");
            let rts = columns[3].as_any().downcast_ref::<Int64Array>().expect("rt column");
            let mask: BooleanArray = (0..ks.len())
                .map(|row| {
                    Some(
                        rts.is_valid(row)
                            && rts.value(row) <= bound
                            && !self.region.contains(&Self::composite_key(
                                ks.value(row),
                                seqs.value(row),
                            )),
                    )
                })
                .collect();
            let normalized =
                RecordBatch::try_new(Arc::new(Schema::new(expected.clone())), columns)
                    .expect("evict normalized batch");
            let filtered = filter_record_batch(&normalized, &mask)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            if filtered.num_rows() > 0 {
                rows.push(filtered);
            }
        }
        for batch in self.region.live_upserts(Some((i64::MIN, bound)))? {
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns() - 1);
            columns.push(batch.column(0).clone());
            columns.extend(batch.columns()[2..].iter().cloned());
            rows.push(
                RecordBatch::try_new(Arc::new(Schema::new(self.arrow_fields())), columns)
                    .expect("region evict batch"),
            );
        }
        let merged = self.merge_by_seq(rows)?;
        if let Some(batch) = &merged {
            let ks = batch.column(1).as_any().downcast_ref::<BinaryArray>().expect("k column");
            let seqs = batch.column(2).as_any().downcast_ref::<Int64Array>().expect("seq column");
            let composites: Vec<Vec<u8>> = (0..batch.num_rows())
                .map(|row| Self::composite_key(ks.value(row), seqs.value(row)))
                .collect();
            let composite_slices: Vec<&[u8]> = composites.iter().map(|c| c.as_slice()).collect();
            let key_groups: Vec<i32> =
                (0..batch.num_rows()).map(|row| self.core.key_group(ks.value(row))).collect();
            let mut values: Vec<ArrayRef> = vec![
                Arc::new(BinaryArray::from_iter_values(
                    (0..batch.num_rows()).map(|row| ks.value(row)),
                )),
                batch.column(2).clone(),
                new_null_array(&DataType::Int64, batch.num_rows()),
                new_null_array(&DataType::Boolean, batch.num_rows()),
            ];
            for field in &self.payload_fields {
                values.push(new_null_array(field.data_type(), batch.num_rows()));
            }
            self.region.append_deletes(&composite_slices, &key_groups, values)?;
        }
        Ok(merged)
    }

    /// Re-stages fully-formed store-schema rows (a probe result's matched rows) with
    /// `matched = true` — the read-modify-write that makes a committed row's matched flag stick.
    pub(crate) fn mark_matched(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let ks = batch.column(1).as_any().downcast_ref::<BinaryArray>().expect("k column");
        let seqs = batch.column(2).as_any().downcast_ref::<Int64Array>().expect("seq column");
        let composites: Vec<Vec<u8>> = (0..batch.num_rows())
            .map(|row| Self::composite_key(ks.value(row), seqs.value(row)))
            .collect();
        let composite_slices: Vec<&[u8]> = composites.iter().map(|c| c.as_slice()).collect();
        let key_groups: Vec<i32> =
            (0..batch.num_rows()).map(|row| self.core.key_group(ks.value(row))).collect();
        let mut values: Vec<ArrayRef> = vec![
            batch.column(1).clone(),
            batch.column(2).clone(),
            batch.column(3).clone(),
            Arc::new(arrow::array::BooleanArray::from(vec![true; batch.num_rows()])),
        ];
        values.extend(batch.columns()[5..].iter().cloned());
        self.region.append_upserts(&composite_slices, &key_groups, values)
    }

    /// Concatenates store-schema batches and sorts by the arrival sequence.
    fn merge_by_seq(
        &self,
        rows: Vec<RecordBatch>,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        if rows.is_empty() {
            return Ok(None);
        }
        let schema = Arc::new(Schema::new(self.arrow_fields()));
        let merged = concat_batches(&schema, &rows)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if merged.num_rows() == 0 {
            return Ok(None);
        }
        let seqs = merged.column(2).as_any().downcast_ref::<Int64Array>().expect("seq column");
        let mut order: Vec<u32> = (0..merged.num_rows() as u32).collect();
        order.sort_by_key(|&row| seqs.value(row as usize));
        let indices = UInt32Array::from(order);
        let columns: Vec<ArrayRef> = merged
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).expect("interval merge sort"))
            .collect();
        Ok(Some(
            RecordBatch::try_new(merged.schema(), columns).expect("interval merged batch"),
        ))
    }

    /// Checkpoint sync phase: commits the region (staged rows, matched-flag rewrites, and
    /// evictions) as this table's snapshot.
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
                        .expect("interval-side write batch")
                })
                .collect();
            self.core.commit_batches(&write_batches)?;
        }
        self.region.clear();
        self.core.checkpoint_manifest()
    }
}

/// Both sides of an interval join under one operator directory (`left/`, `right/`) — see
/// [`PaimonIntervalSideStore`]. The snapshot token packs both snapshot ids and both arrival
/// sequences.
pub(crate) struct PaimonIntervalJoinStore {
    pub(crate) left: PaimonIntervalSideStore,
    pub(crate) right: PaimonIntervalSideStore,
    last_footprint: usize,
}

impl PaimonIntervalJoinStore {
    pub(crate) fn create(
        config: PaimonStoreConfig,
        left_types: Vec<DataType>,
        right_types: Vec<DataType>,
    ) -> Result<Self, DataFusionError> {
        Ok(PaimonIntervalJoinStore {
            left: PaimonIntervalSideStore::create(
                PaimonOverStore::side_config(&config, "left"),
                left_types,
            )?,
            right: PaimonIntervalSideStore::create(
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
        Ok(PaimonIntervalJoinStore {
            left: PaimonIntervalSideStore::open_merged(
                PaimonOverStore::side_config(&config, "left"),
                left_types,
                left_sources,
                key_groups.clone(),
                aligned,
            )?,
            right: PaimonIntervalSideStore::open_merged(
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

    /// Checkpoint sync phase: commits both sides; the caller packs the two manifests and both
    /// arrival sequences into the snapshot token.
    pub(crate) fn checkpoint(
        &mut self,
    ) -> Result<(PaimonCheckpointManifest, PaimonCheckpointManifest), DataFusionError> {
        Ok((self.left.checkpoint()?, self.right.checkpoint()?))
    }
}
