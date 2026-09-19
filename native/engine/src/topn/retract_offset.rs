use super::*;

/// Flink's hidden-rank cascade retains both mutated row kinds and sort-key counts. A failed
/// full-row retraction decrements the count anyway, so deriving counts from list lengths loses
/// information. The count lives on the first row of each contiguous tie group, including zero
/// counts whose payload list still exists. Older snapshots infer counts from their list lengths.
#[allow(clippy::too_many_arguments)]
pub(super) fn push(
    buffer: &mut Vec<TopNRow>,
    mut input: TopNRow,
    retract: bool,
    offset: i64,
    limit: i64,
    generate_update_before: bool,
    out_rows: &mut Vec<Arc<OwnedRow>>,
    out_kinds: &mut Vec<i8>,
) -> isize {
    let start = buffer.partition_point(|row| row.sort < input.sort);
    let end = buffer.partition_point(|row| row.sort <= input.sort);
    let count = if start < end {
        buffer[start].sort_count.unwrap_or((end - start) as i64)
    } else {
        0
    };
    let mut bytes = 0;
    if !retract {
        bytes += topn_entry_bytes(&input) as isize;
        if start == end {
            input.sort_count = Some(1);
        } else {
            buffer[start].sort_count = Some(count + 1);
        }
        buffer.insert(end, input.clone());
    }

    let mut emit = |buffer: &mut [TopNRow], index: usize, rank: i64, kind: i8| {
        if rank > offset && rank <= limit && (kind != 1 || generate_update_before) {
            buffer[index].stored_kind = kind;
            out_rows.push(Arc::clone(&buffer[index].payload));
            out_kinds.push(kind);
        }
    };
    let mut rank = 0;
    let mut current = None;
    let mut removed = None;
    let mut group_start = 0;
    while group_start < buffer.len() && rank <= limit {
        let group_end = group_start
            + buffer[group_start..].partition_point(|row| row.sort == buffer[group_start].sort);
        let group_count = buffer[group_start]
            .sort_count
            .unwrap_or((group_end - group_start) as i64);
        // A zero count removes this key from Flink's sorted map, but not from its data state.
        if group_count > 0 {
            let matches_key = buffer[group_start].sort == input.sort;
            if !retract && current.is_none() && matches_key {
                rank += group_count;
                current = Some(end);
            } else if current.is_some() || (retract && matches_key) {
                for index in group_start..group_end {
                    if rank > limit {
                        break;
                    }
                    if retract && current.is_none() {
                        if buffer[index].stored_kind == 0 && buffer[index].payload == input.payload
                        {
                            current = Some(index);
                            removed = Some(index);
                        }
                    } else if let Some(previous) = current {
                        if retract {
                            emit(buffer, previous, rank, 1);
                            emit(buffer, index, rank, 2);
                        } else {
                            emit(buffer, index, rank, 1);
                            emit(buffer, previous, rank, 2);
                        }
                        current = Some(index);
                    }
                    rank += 1;
                }
            } else {
                rank += group_count;
            }
        }
        group_start = group_end;
    }
    if let Some(index) = current {
        emit(buffer, index, rank, if retract { 3 } else { 0 });
    }

    if retract && start < end {
        // The count changes even when equality fails because a previous emission changed a kind.
        let remaining = (count - 1).max(0);
        buffer[start].sort_count = Some(remaining);
        let removed = removed.or_else(|| {
            (start..end).find(|&index| {
                buffer[index].stored_kind == 0 && buffer[index].payload == input.payload
            })
        });
        if let Some(index) = removed {
            bytes -= topn_entry_bytes(&buffer[index]) as isize;
            buffer.remove(index);
            if index == start && start + 1 < end {
                buffer[start].sort_count = Some(remaining);
            }
        }
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_snapshot_kinds_survive_without_explicit_counts() {
        let batch = |values: Vec<i64>, kinds: Vec<i8>| {
            RecordBatch::try_from_iter(vec![
                ("v", Arc::new(Int64Array::from(values)) as ArrayRef),
                (
                    ROW_KIND_COLUMN,
                    Arc::new(Int8Array::from(kinds)) as ArrayRef,
                ),
            ])
            .unwrap()
        };
        let sort = vec![SortColumn {
            index: 0,
            ascending: true,
            nulls_first: false,
        }];
        let mut legacy = TopNRanker::new(vec![], sort.clone(), 100, false, false);
        legacy
            .push(&batch(vec![10, 20, 30], vec![0, 0, 0]), 0)
            .unwrap();
        let restore = |bytes: &[u8]| {
            RetractableTopNRanker::restore(vec![], vec![], sort.clone(), 1, 3, false, bytes, 0)
        };
        let mut ranker = restore(&legacy.snapshot());
        ranker.push(&batch(vec![5, 5], vec![0, 3]), 0).unwrap();
        let retained: Vec<_> = ranker
            .groups
            .iter()
            .flat_map(|(_, rows)| rows.iter())
            .collect();
        assert!(retained.iter().all(|row| row.sort_count.is_none()));
        assert!(retained.iter().any(|row| row.stored_kind != 0));
        let mut restored = restore(&ranker.snapshot());
        let changes = batch(vec![20, 15, 10], vec![3, 0, 1]);
        assert_eq!(
            ranker.push(&changes, 0).unwrap(),
            restored.push(&changes, 0).unwrap()
        );

        #[cfg(feature = "rocksdb-state")]
        {
            use crate::state::RocksStateCodec;
            let codec = TopNStateCodec::new(ranker.converters.as_ref().unwrap());
            for (_, rows) in ranker.groups.iter() {
                let mut encoded = Vec::new();
                codec.raw_write(rows, &mut encoded);
                assert_eq!(encoded.len(), codec.value_bytes(rows));
                let decoded = codec.from_raw(&encoded);
                assert_eq!(
                    rows.iter()
                        .map(|row| (row.stored_kind, row.sort_count))
                        .collect::<Vec<_>>(),
                    decoded
                        .iter()
                        .map(|row| (row.stored_kind, row.sort_count))
                        .collect::<Vec<_>>()
                );
            }
        }
    }
}
