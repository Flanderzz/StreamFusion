use super::*;

#[derive(Debug)]
pub(super) struct FloatingExtremeAccumulator {
    agg: RunningAgg,
}

impl FloatingExtremeAccumulator {
    pub(super) fn new(kind: i64, datatype: &DataType) -> Self {
        Self {
            agg: RunningAgg::new(kind, datatype),
        }
    }
}

impl Accumulator for FloatingExtremeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        use datafusion::common::cast::{as_float32_array, as_float64_array};
        match self.agg.result_type() {
            DataType::Float32 => {
                for value in as_float32_array(&values[0])?.iter().flatten() {
                    self.agg.fold(Num::F32(value));
                }
            }
            _ => {
                for value in as_float64_array(&values[0])?.iter().flatten() {
                    self.agg.fold(Num::F64(value));
                }
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![self.agg.emit()])
    }
    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok(self.agg.emit())
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;

    #[test]
    fn merged_extrema_keep_the_first_nan_and_zero_sign() {
        for kind in [1, 2] {
            for (first, second) in [(f64::NAN, 2.0), (2.0, f64::NAN), (0.0, -0.0), (-0.0, 0.0)] {
                let mut agg = FloatingExtremeAccumulator::new(kind, &DataType::Float64);
                agg.update_batch(&[Arc::new(Float64Array::from(vec![None, Some(first)]))])
                    .unwrap();
                let snapshot = agg.state().unwrap()[0].to_array().unwrap();
                let mut restored = FloatingExtremeAccumulator::new(kind, &DataType::Float64);
                restored.merge_batch(&[snapshot]).unwrap();
                restored
                    .update_batch(&[Arc::new(Float64Array::from(vec![Some(second), None]))])
                    .unwrap();
                let ScalarValue::Float64(Some(actual)) = restored.evaluate().unwrap() else {
                    panic!("double result")
                };
                assert_eq!(actual.to_bits(), first.to_bits());
            }
        }
    }
}
