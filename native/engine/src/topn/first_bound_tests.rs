use super::*;
use arrow::array::Float64Array;

fn batch(rows: &[(i64, f64, i64)]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
        ),
        (
            "score",
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.1))) as ArrayRef,
        ),
        (
            "bound",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn ranker(rank: bool, ttl: i64) -> TopNRanker {
    let mut ranker = TopNRanker::new(
        vec![0],
        vec![SortColumn {
            index: 1,
            ascending: true,
            nulls_first: true,
        }],
        i64::MAX,
        rank,
        false,
    )
    .with_rank_end_column(2)
    .with_state_ttl(ttl);
    ranker.enable_first_bound(true);
    ranker
}

fn restored(ranker: &TopNRanker, now: i64) -> TopNRanker {
    let snapshots: Vec<_> = ranker.snapshot_partitions(128).into_values().collect();
    let mut restored = TopNRanker::restore_partitions(
        vec![0],
        vec![-1],
        vec![SortColumn {
            index: 1,
            ascending: true,
            nulls_first: true,
        }],
        i64::MAX,
        ranker.output_rank_number,
        false,
        &snapshots,
        now,
    )
    .with_rank_end_column(2)
    .with_state_ttl(ranker.ttl_ms);
    restored.enable_first_bound(true);
    restored
}

#[test]
fn bound_only_keys_survive_snapshot_and_expire_without_refresh() {
    for projected in [false, true] {
        let mut ranker = ranker(projected, 1000);
        assert_eq!(
            ranker
                .push(&batch(&[(1, 20.0, -1), (2, 20.0, 0)]), 5000)
                .unwrap()
                .num_rows(),
            0
        );
        let mut ranker = restored(&ranker, 5500);
        assert_eq!(ranker.groups.iter().count(), 2);
        assert_eq!(
            ranker
                .push(&batch(&[(1, 10.0, 2), (2, 10.0, 2)]), 5999)
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(ranker.invalid_top_size, 2);
        let output = ranker
            .push(&batch(&[(1, 5.0, 2), (2, 5.0, 2)]), 6000)
            .unwrap();
        // At N=0 the projected path retained an un-emitted overflow row; widening
        // reproduces Flink's positional update pair before the new rank insert.
        assert_eq!(output.num_rows(), if projected { 4 } else { 2 });
        for (_, state) in ranker.groups.iter() {
            assert_eq!(state.first_rank_end.unwrap().value, 2);
            assert_eq!(state.first_rank_end.unwrap().written_at, 6000);
        }
    }
}

#[test]
fn bound_expires_while_refreshed_tie_rows_are_live() {
    let mut ranker = ranker(true, 1000);
    ranker.push(&batch(&[(1, 20.0, 1)]), 5000).unwrap();
    ranker.push(&batch(&[(1, 20.0, 1)]), 5700).unwrap();
    let state = ranker.groups.iter().next().unwrap().1;
    assert_eq!(state.len(), 2, "retain the overflow tie group");
    assert!(state.iter().all(|row| row.ts_ms == 5700));
    let mut ranker = restored(&ranker, 5900);
    let output = ranker.push(&batch(&[(1, 10.0, 2)]), 6000).unwrap();
    assert_eq!(output.num_rows(), 4);
    assert_eq!(row_kind_column(&output).unwrap().values(), &[1, 2, 1, 2]);
    assert_eq!(
        ranker
            .groups
            .iter()
            .next()
            .unwrap()
            .1
            .first_rank_end
            .unwrap()
            .value,
        2
    );
    assert_eq!(ranker.invalid_top_size, 0);
}

#[test]
fn asynchronous_snapshot_freezes_bound_and_rows_together() {
    let mut ranker = ranker(false, 1000);
    ranker
        .push(&batch(&[(1, 10.0, 0), (2, 10.0, 1)]), 5000)
        .unwrap();
    let snapshot = AppendTopNSnapshot::capture(&ranker, 128).unwrap();
    ranker
        .push(&batch(&[(1, 5.0, 3), (2, 5.0, 3)]), 6000)
        .unwrap();
    let blobs: Vec<_> = snapshot
        .partitions
        .keys()
        .map(|&kg| snapshot.encode(kg).unwrap())
        .collect();
    let mut restored = TopNRanker::restore_partitions(
        vec![0],
        vec![-1],
        vec![SortColumn {
            index: 1,
            ascending: true,
            nulls_first: true,
        }],
        i64::MAX,
        false,
        false,
        &blobs,
        5500,
    )
    .with_rank_end_column(2)
    .with_state_ttl(1000);
    restored.enable_first_bound(true);
    let out = restored
        .push(&batch(&[(1, 1.0, 3), (2, 1.0, 3)]), 5500)
        .unwrap();
    assert_eq!(out.num_rows(), 2);
    assert_eq!(restored.invalid_top_size, 2);
    assert!(restored
        .groups
        .iter()
        .all(|(_, s)| s.first_rank_end.unwrap().written_at == 5000));
}

#[test]
fn bound_only_state_obeys_the_memory_budget() {
    let mut ranker = ranker(false, 0).with_memory_budget(1).unwrap();
    assert!(ranker.push(&batch(&[(1, 1.0, 0)]), 0).is_err());
}

#[test]
fn enabling_ttl_on_restore_starts_the_bound_clock_at_restore() {
    let mut before = ranker(false, 0);
    before.push(&batch(&[(1, 1.0, 0)]), 17).unwrap();
    let mut after = restored(&before, 5000).with_state_ttl(1000);
    assert_eq!(
        after.push(&batch(&[(1, 2.0, 2)]), 5999).unwrap().num_rows(),
        0
    );
    assert_eq!(after.invalid_top_size, 1);
    assert_eq!(
        after.push(&batch(&[(1, 3.0, 2)]), 6000).unwrap().num_rows(),
        1
    );
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn persistent_codec_preserves_independent_clocks_and_empty_buffers() {
    use crate::state::RocksStateCodec;
    for rows in [vec![(1, 20.0, 0)], vec![(1, -0.0, 2), (1, 0.0, 3)]] {
        let mut ranker = ranker(false, 1000);
        ranker.push(&batch(&rows), 5000).unwrap();
        let codec = TopNStateCodec::new(ranker.converters.as_ref().unwrap());
        for (_, state) in ranker.groups.iter() {
            let mut bytes = Vec::new();
            codec.raw_write(state, &mut bytes);
            assert_eq!(codec.value_bytes(state), bytes.len());
            let restored = codec.from_raw(&bytes);
            assert_eq!(restored.len(), state.len());
            assert_eq!(
                restored.first_rank_end.unwrap().value,
                state.first_rank_end.unwrap().value
            );
            assert_eq!(codec.write_ms(&restored), 5000);
            for (before, after) in state.iter().zip(restored.iter()) {
                assert_eq!(before.payload, after.payload);
            }
        }
    }
}
