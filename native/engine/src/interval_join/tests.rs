use super::*;
use arrow::array::TimestampSecondArray;
use arrow::datatypes::TimeUnit;
fn batch(times: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from_value(1, times.len())) as ArrayRef,
        ),
        ("rt", times),
    ])
    .unwrap()
}

fn joiner(
    left: &RecordBatch,
    right: &RecordBatch,
    lower: i64,
    upper: i64,
    kind: JoinKind,
) -> IntervalJoiner {
    IntervalJoiner::new(
        vec![0],
        vec![0],
        1,
        1,
        lower,
        upper,
        None,
        kind,
        left.schema(),
        right.schema(),
    )
}

#[test]
fn mixed_layout_bounds_keep_payloads_and_match_flags_after_restore() {
    let layouts: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1000])),
        Arc::new(TimestampSecondArray::from(vec![1])),
        Arc::new(TimestampMillisecondArray::from(vec![1000])),
        Arc::new(TimestampMicrosecondArray::from(vec![1_000_999])),
        Arc::new(TimestampNanosecondArray::from(vec![1_000_999_999])),
    ];
    for left in layouts {
        let left = batch(left);
        let right = batch(Arc::new(TimestampNanosecondArray::from(vec![
            1_000_000_001,
        ])));
        for kind in [
            JoinKind::Inner,
            JoinKind::LeftOuter,
            JoinKind::RightOuter,
            JoinKind::FullOuter,
        ] {
            for left_first in [true, false] {
                let mut original = joiner(&left, &right, 0, 0, kind);
                if left_first {
                    original.push_left(left.clone(), None).unwrap();
                } else {
                    original.push_right(right.clone(), None).unwrap();
                }
                let mut restored = IntervalJoiner::restore(
                    vec![0],
                    vec![0],
                    1,
                    1,
                    0,
                    0,
                    None,
                    kind,
                    left.schema(),
                    right.schema(),
                    &original.snapshot(),
                );
                let output = if left_first {
                    restored.push_right(right.clone(), None).unwrap()
                } else {
                    restored.push_left(left.clone(), None).unwrap()
                };
                assert_eq!(output.num_rows(), 1, "{}", left.column(1).data_type());
                assert_eq!(output.column(1), left.column(1));
                assert_eq!(output.column(3), right.column(1));
                let mut restored = IntervalJoiner::restore(
                    vec![0],
                    vec![0],
                    1,
                    1,
                    0,
                    0,
                    None,
                    kind,
                    left.schema(),
                    right.schema(),
                    &restored.snapshot(),
                );
                assert_eq!(restored.advance(1000).unwrap().num_rows(), 0);
                assert!(restored.left_buffered.is_empty());
                assert!(restored.right_buffered.is_empty());
            }
        }
    }
}

#[test]
fn wide_timestamps_and_offsets_do_not_require_nanosecond_durations() {
    for millis in [i64::MIN + 2000, 31_494_784_780_800_000, i64::MAX - 2000] {
        let left = batch(Arc::new(TimestampMillisecondArray::from(vec![millis])));
        let right = batch(Arc::new(TimestampMillisecondArray::from(vec![
            millis - 1000,
        ])));
        for left_first in [true, false] {
            let mut join = joiner(&left, &right, -1000, 1000, JoinKind::Inner);
            let output = if left_first {
                join.push_left(left.clone(), None).unwrap();
                join.push_right(right.clone(), None).unwrap()
            } else {
                join.push_right(right.clone(), None).unwrap();
                join.push_left(left.clone(), None).unwrap()
            };
            assert_eq!(output.num_rows(), 1);
            assert_eq!(output.column(1), left.column(1));
            assert_eq!(output.column(3), right.column(1));
            join.advance(i64::MAX).unwrap();
        }
    }
    let left = batch(Arc::new(TimestampNanosecondArray::from(vec![
        1_000_000_000,
    ])));
    let right = batch(Arc::new(TimestampNanosecondArray::from(vec![
        5_000_000_000,
    ])));
    let mut join = joiner(
        &left,
        &right,
        -12_000_000_000_000,
        12_000_000_000_000,
        JoinKind::Inner,
    );
    join.push_left(left, None).unwrap();
    assert_eq!(join.push_right(right, None).unwrap().num_rows(), 1);
}

#[test]
fn bounds_preserve_java_overflow_direction_and_sql_nulls() {
    let bounds = IntervalBounds {
        left_time: 0,
        right_time: 0,
        lower: 0,
        upper: 10,
        incoming_left: true,
    };
    assert!(bounds.contains(i64::MAX - 5, i64::MAX - 7));
    assert!(!IntervalBounds {
        incoming_left: false,
        ..bounds
    }
    .contains(i64::MAX - 5, i64::MAX - 7));
    let left: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
        Some(-1),
        None,
        Some(1),
    ]));
    let right = Arc::new(TimestampMillisecondArray::from(vec![
        Some(-1),
        Some(0),
        None,
    ])) as ArrayRef;
    let input = RecordBatch::try_from_iter(vec![("left", left), ("right", right)]).unwrap();
    let result = IntervalBounds { upper: 0, ..bounds }
        .expression(&input.schema(), 1)
        .evaluate(&input)
        .unwrap()
        .into_array(3)
        .unwrap();
    assert_eq!(
        result.as_ref(),
        &BooleanArray::from(vec![Some(true), None, None])
    );
    let predicate = IntervalPredicate {
        bounds,
        signature: Signature::any(2, Volatility::Immutable),
    };
    assert!(predicate
        .return_type(&[
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Int64
        ])
        .is_ok());
    assert!(predicate
        .return_type(&[DataType::Utf8, DataType::Int64])
        .is_err());
    assert!(predicate.return_type(&[]).is_err());
}
