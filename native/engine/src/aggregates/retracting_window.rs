use crate::*;

pub(crate) const RETRACT_SUM: i64 = 9;
pub(crate) const RETRACT_COUNT: i64 = 10;
pub(crate) const LIVE_COUNT: i64 = 11;
pub(crate) const HIDDEN_LIVE_COUNT: i64 = 12;

#[derive(Debug)]
pub(super) struct RetractingWindowAccumulator {
    sum_type: Option<DataType>,
    sum: Option<i64>,
    count: i64,
}

impl RetractingWindowAccumulator {
    pub(super) fn new(sum_type: Option<DataType>) -> Self {
        Self {
            sum_type,
            sum: None,
            count: 0,
        }
    }

    fn sum_scalar(&self, value: Option<i64>) -> ScalarValue {
        match self.sum_type.as_ref().expect("sum accumulator") {
            DataType::Int8 => ScalarValue::Int8(value.map(|v| v as i8)),
            DataType::Int16 => ScalarValue::Int16(value.map(|v| v as i16)),
            DataType::Int32 => ScalarValue::Int32(value.map(|v| v as i32)),
            DataType::Int64 => ScalarValue::Int64(value),
            other => unreachable!("unsupported retracting SUM: {other}"),
        }
    }

    fn integer(array: &ArrayRef, row: usize) -> i64 {
        match array.data_type() {
            DataType::Int8 => array
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(row) as i64,
            DataType::Int16 => array
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row) as i64,
            DataType::Int32 => array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row) as i64,
            DataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
            other => unreachable!("unsupported retracting SUM column: {other}"),
        }
    }
}

impl Accumulator for RetractingWindowAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let input = &values[0];
        let changes = values
            .get(1)
            .map(|v| v.as_any().downcast_ref::<Int8Array>().unwrap());
        for row in 0..input.len() {
            if input.is_null(row) {
                continue;
            }
            let retract = changes.is_some_and(|kinds| matches!(kinds.value(row), 1 | 3));
            if self.sum_type.is_some() {
                let value = Self::integer(input, row);
                let sum = self.sum.unwrap_or(0);
                self.sum = Some(if retract {
                    sum.wrapping_sub(value)
                } else {
                    sum.wrapping_add(value)
                });
            }
            self.count = if retract {
                self.count.wrapping_sub(1)
            } else {
                self.count.wrapping_add(1)
            };
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        let count_column = usize::from(self.sum_type.is_some());
        let counts = states[count_column]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..counts.len() {
            if self.sum_type.is_some() && !states[0].is_null(row) {
                self.sum = Some(
                    self.sum
                        .unwrap_or(0)
                        .wrapping_add(Self::integer(&states[0], row)),
                );
            }
            self.count = self.count.wrapping_add(counts.value(row));
        }
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        let count = ScalarValue::Int64(Some(self.count));
        Ok(if self.sum_type.is_some() {
            vec![self.sum_scalar(self.sum), count]
        } else {
            vec![count]
        })
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok(if self.sum_type.is_some() {
            // Flink tests zero, not positivity: unmatched deletes have a negative count and sum.
            self.sum_scalar(if self.count == 0 { None } else { self.sum })
        } else {
            ScalarValue::Int64(Some(self.count))
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_retract_buffers_wrap_at_every_declared_width_and_keep_zero_count_null() {
        for (data_type, max) in [
            (DataType::Int8, i8::MAX as i64),
            (DataType::Int16, i16::MAX as i64),
            (DataType::Int32, i32::MAX as i64),
            (DataType::Int64, i64::MAX),
        ] {
            let values = |values: Vec<Option<i64>>| {
                arrow::compute::cast(&Int64Array::from(values), &data_type).unwrap()
            };
            let mut first = RetractingWindowAccumulator::new(Some(data_type.clone()));
            first
                .update_batch(&[values(vec![Some(max), Some(1), None])])
                .unwrap();
            let state = first
                .state()
                .unwrap()
                .into_iter()
                .map(|v| v.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>();
            let mut restored = RetractingWindowAccumulator::new(Some(data_type.clone()));
            restored.merge_batch(&state).unwrap();
            restored
                .update_batch(&[
                    values(vec![Some(max), None]),
                    Arc::new(Int8Array::from(vec![3, 3])),
                ])
                .unwrap();
            assert_eq!(restored.evaluate().unwrap(), restored.sum_scalar(Some(1)));
            restored
                .update_batch(&[values(vec![Some(1)]), Arc::new(Int8Array::from(vec![1]))])
                .unwrap();
            assert_eq!(restored.evaluate().unwrap(), restored.sum_scalar(None));
            assert_eq!(restored.state().unwrap()[1], ScalarValue::Int64(Some(0)));
            restored
                .update_batch(&[values(vec![Some(3)]), Arc::new(Int8Array::from(vec![3]))])
                .unwrap();
            assert_eq!(restored.evaluate().unwrap(), restored.sum_scalar(Some(-3)));
            assert_eq!(restored.state().unwrap()[1], ScalarValue::Int64(Some(-1)));
        }
    }
}
