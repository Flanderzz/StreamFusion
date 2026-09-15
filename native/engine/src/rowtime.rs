use crate::*;
use streamfusion_bridge::timestamp::TimestampColumn;

/// Max of a rowtime column in epoch millis, or `i64::MIN` when every value is null. The JVM side
/// treats `i64::MIN` as Flink's `NO_TIMESTAMP` sentinel. BIGINT rowtimes already hold epoch millis.
pub(crate) fn max_rowtime_millis(batch: &RecordBatch, index: usize) -> i64 {
    let column = batch.column(index);
    match column.data_type() {
        DataType::Timestamp(_, _) => TimestampColumn::try_new(column.as_ref())
            .expect("timestamp rowtime column")
            .max_millis()
            .expect("rowtime in Flink's millisecond range")
            .unwrap_or(i64::MIN),
        DataType::Int64 => {
            let array = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("bigint rowtime downcast");
            arrow::compute::max(array).unwrap_or(i64::MIN)
        }
        other => panic!("unsupported rowtime column type {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::TimestampSecondArray;

    #[test]
    fn max_reads_all_units_and_preserves_the_valid_minimum() {
        for (array, expected) in [
            (
                Arc::new(TimestampSecondArray::from(vec![None, Some(-1)])) as ArrayRef,
                -1000,
            ),
            (
                Arc::new(TimestampMillisecondArray::from(vec![None, Some(i64::MAX)])),
                i64::MAX,
            ),
            (
                Arc::new(TimestampMicrosecondArray::from(vec![None, Some(-1)])),
                -1,
            ),
            (
                Arc::new(TimestampNanosecondArray::from(vec![Some(i64::MIN)])),
                -9_223_372_036_855,
            ),
            (Arc::new(TimestampNanosecondArray::new_null(2)), i64::MIN),
        ] {
            let batch = RecordBatch::try_from_iter(vec![("rt", array)]).unwrap();
            assert_eq!(max_rowtime_millis(&batch, 0), expected);
        }
    }
}
