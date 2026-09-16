use super::*;

#[test]
fn fixed_offset_assignment_preserves_payload_and_instant_window_time() {
    use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};
    let values = vec![
        Some(TimestampValue::new(-1, 999_999).unwrap()),
        Some(TimestampValue::new(0, 123_456).unwrap()),
        Some(TimestampValue::from_millis(253_402_300_790_000)),
        None,
    ];
    let input = RecordBatch::try_from_iter(vec![(
        "ts",
        Arc::new(timestamp_array(values.clone())) as ArrayRef,
    )])
    .unwrap();
    for offset in [0, 28_800_000, -19_800_000] {
        for (slide, cumulative) in [(10_000, false), (5_000, false), (5_000, true)] {
            let assigned = assign_windows(
                &input,
                0,
                10_000,
                slide,
                cumulative,
                None,
                &streamfusion_bridge::timestamp::timestamp_type(),
                offset,
            )
            .unwrap();
            let payload = TimestampColumn::try_new(assigned.column(0).as_ref()).unwrap();
            let starts = TimestampColumn::try_new(assigned.column(1).as_ref())
                .unwrap()
                .to_millis()
                .unwrap();
            let ends = TimestampColumn::try_new(assigned.column(2).as_ref())
                .unwrap()
                .to_millis()
                .unwrap();
            let times = TimestampColumn::try_new(assigned.column(3).as_ref())
                .unwrap()
                .to_millis()
                .unwrap();
            for row in 0..assigned.num_rows() {
                let value = payload.value(row).unwrap();
                assert!(values.contains(&Some(value)));
                assert!(starts.value(row) <= value.millis() + offset);
                assert!(value.millis() + offset < ends.value(row));
                assert_eq!(times.value(row), ends.value(row) - offset - 1);
                assert_eq!(starts.value(row) % slide, 0);
            }
        }
    }
}
use arrow::array::TimestampSecondArray;
use arrow::datatypes::TimeUnit;

fn input(times: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from_value(7, times.len())) as ArrayRef,
        ),
        ("rt", times),
        (
            ROW_KIND_COLUMN,
            Arc::new(Int8Array::from_value(0, 3)) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn assignment_reads_all_layouts_and_keeps_payload_and_changelog() {
    let times: Vec<ArrayRef> = vec![
        Arc::new(TimestampSecondArray::from(vec![Some(-1), None, Some(1)])),
        Arc::new(TimestampMillisecondArray::from(vec![
            Some(-1),
            None,
            Some(1000),
        ])),
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(-1),
            None,
            Some(1_000_001),
        ])),
        Arc::new(TimestampNanosecondArray::from(vec![
            Some(-1),
            None,
            Some(1_000_000_001),
        ])),
    ];
    for times in times {
        let batch = input(times);
        for (size, step, cumulative) in
            [(1000, 1000, false), (2000, 1000, false), (2000, 1000, true)]
        {
            let out = assign_windows(
                &batch,
                1,
                size,
                step,
                cumulative,
                None,
                &DataType::Timestamp(TimeUnit::Nanosecond, None),
                0,
            )
            .unwrap();
            let expected_indices = if cumulative || size == step {
                vec![0, 2]
            } else {
                vec![0, 0, 2, 2]
            };
            let original =
                take(batch.column(1), &UInt32Array::from(expected_indices), None).unwrap();
            assert_eq!(out.column(1).to_data(), original.to_data());
            assert_eq!(out.schema().field(5).name(), ROW_KIND_COLUMN);
            for column in out.columns() {
                column.to_data().validate_full().unwrap();
            }
            let starts = TimestampColumn::try_new(out.column(2).as_ref())
                .unwrap()
                .to_millis()
                .unwrap();
            let ends = TimestampColumn::try_new(out.column(3).as_ref())
                .unwrap()
                .to_millis()
                .unwrap();
            assert_eq!(starts.value(0), if cumulative { -2000 } else { -1000 });
            assert_eq!(
                ends.value(0),
                if size == 2000 && !cumulative { 1000 } else { 0 }
            );
            let window_time = TimestampColumn::try_new(out.column(4).as_ref()).unwrap();
            for row in 0..out.num_rows() {
                assert_eq!(
                    window_time.value(row).unwrap().millis(),
                    ends.value(row) - 1
                );
                assert_eq!(window_time.value(row).unwrap().nano_of_milli(), 0);
            }
        }
    }
}

#[test]
fn boundary_output_layout_is_explicit_and_never_wraps() {
    let batch = input(Arc::new(TimestampNanosecondArray::from(vec![-1; 3])));
    for unit in [
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        let ty = DataType::Timestamp(unit, None);
        let out = assign_windows(&batch, 1, 1000, 1000, false, None, &ty, 0).unwrap();
        assert_eq!(out.column(2).data_type(), &ty);
        assert_eq!(
            TimestampColumn::try_new(out.column(4).as_ref())
                .unwrap()
                .value(0)
                .unwrap()
                .millis(),
            -1
        );
    }
    assert!(assign_windows(
        &batch,
        1,
        1000,
        1000,
        false,
        None,
        &DataType::Timestamp(TimeUnit::Second, None),
        0
    )
    .is_err());
    let wide = input(Arc::new(TimestampMillisecondArray::from(
        vec![10_000_000_000_000; 3],
    )));
    assert!(assign_windows(
        &wide,
        1,
        1000,
        1000,
        false,
        None,
        &DataType::Timestamp(TimeUnit::Nanosecond, None),
        0
    )
    .is_err());
}

#[test]
fn proctime_assignment_ignores_null_payload_time() {
    let batch = input(Arc::new(TimestampNanosecondArray::new_null(3)));
    let out = assign_windows(
        &batch,
        1,
        1000,
        1000,
        false,
        Some(-1),
        &DataType::Timestamp(TimeUnit::Nanosecond, None),
        0,
    )
    .unwrap();
    assert_eq!(out.num_rows(), 3);
    assert_eq!(out.column(1).null_count(), 3);
    assert_eq!(
        TimestampColumn::try_new(out.column(2).as_ref())
            .unwrap()
            .value(0)
            .unwrap()
            .millis(),
        -1000
    );
}

#[test]
fn tvf_output_survives_downstream_window_join_restore() {
    let millis: i64 = -1;
    let batch = input(Arc::new(TimestampNanosecondArray::from(vec![
        millis * 1_000_000
            + 999999;
        3
    ])))
    .slice(1, 1);
    let assigned = assign_windows(
        &batch,
        1,
        1000,
        1000,
        false,
        None,
        &DataType::Timestamp(TimeUnit::Nanosecond, None),
        0,
    )
    .unwrap();
    // Drop only the changelog sidecar, as the operator wrapper does before joining.
    let assigned = assigned.project(&[0, 1, 2, 3, 4]).unwrap();
    let schema = assigned.schema();
    let mut joiner = WindowJoiner::new(
        vec![0],
        vec![0],
        2,
        3,
        2,
        3,
        None,
        JoinKind::Inner,
        schema.clone(),
        schema.clone(),
    );
    joiner.push_left(assigned.clone()).unwrap();
    joiner.push_right(assigned.clone()).unwrap();
    let mut restored = WindowJoiner::restore(
        vec![0],
        vec![0],
        2,
        3,
        2,
        3,
        None,
        JoinKind::Inner,
        schema.clone(),
        schema,
        &joiner.snapshot(),
    );
    let end = millis - millis.rem_euclid(1000) + 1000;
    assert_eq!(restored.flush(end - 1).unwrap().num_rows(), 0);
    let out = restored.flush(end).unwrap();
    assert_eq!(out.num_rows(), 1);
    for column in [1, 6] {
        let time = TimestampColumn::try_new(out.column(column).as_ref())
            .unwrap()
            .value(0)
            .unwrap();
        assert_eq!((time.millis(), time.nano_of_milli()), (millis, 999999));
    }
    restored.push_left(assigned).unwrap();
    assert_eq!(restored.left_late_drops, 1);
}
