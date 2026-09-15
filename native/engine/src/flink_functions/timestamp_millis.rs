//! Millisecond-valued timestamp expressions, independent of the pipeline's timestamp unit.

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Int64Type};
use datafusion::common::{cast::as_primitive_array, exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub(super) enum Operation {
    Millis,
    SubtractMillis,
    SubtractMonths,
}

pub(super) fn function(operation: Operation) -> ScalarUDF {
    ScalarUDF::new_from_impl(TimestampMillis {
        operation,
        signature: Signature::any(
            if operation == Operation::Millis { 1 } else { 2 },
            Volatility::Immutable,
        ),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TimestampMillis {
    operation: Operation,
    signature: Signature,
}

impl ScalarUDFImpl for TimestampMillis {
    fn name(&self) -> &str {
        match self.operation {
            Operation::Millis => "flink_timestamp_millis",
            Operation::SubtractMillis => "flink_timestamp_subtract_millis",
            Operation::SubtractMonths => "flink_timestamp_subtract_months",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        let valid = match (self.operation, types) {
            (Operation::Millis, [DataType::Int64 | DataType::Timestamp(_, None)]) => true,
            (
                Operation::SubtractMillis,
                [DataType::Int64 | DataType::Timestamp(_, None), DataType::Int64],
            ) => true,
            (
                Operation::SubtractMonths,
                [DataType::Int64 | DataType::Timestamp(_, None), DataType::Int32],
            ) => true,
            _ => false,
        };
        if valid {
            Ok(DataType::Int64)
        } else {
            exec_err!("{}: unexpected operand types {types:?}", self.name())
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.return_type(
            &args
                .args
                .iter()
                .map(ColumnarValue::data_type)
                .collect::<Vec<_>>(),
        )?;
        let input = args.args[0].clone().into_array(args.number_rows)?;
        let output: ArrayRef = match self.operation {
            Operation::Millis if input.data_type() == &DataType::Int64 => input,
            Operation::Millis => Arc::new(super::map_timestamp_millis(&input, |value| value)?),
            _ => {
                // The encoder preserves the interval as a scalar; do not expand it to a batch column.
                let shift = match (&self.operation, &args.args[1]) {
                    (
                        Operation::SubtractMillis,
                        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))),
                    ) => *v,
                    (
                        Operation::SubtractMonths,
                        ColumnarValue::Scalar(ScalarValue::Int32(Some(v))),
                    ) => i64::from(*v),
                    _ => return exec_err!("{} requires a non-null interval literal", self.name()),
                };
                let subtract = |value: i64| match self.operation {
                    Operation::SubtractMonths => {
                        super::calendar::add_months(value, (shift as i32).wrapping_neg())
                    }
                    _ => value.wrapping_sub(shift),
                };
                let output: Int64Array = if input.data_type() == &DataType::Int64 {
                    as_primitive_array::<Int64Type>(&input)?.unary(subtract)
                } else {
                    super::map_timestamp_millis(&input, subtract)?
                };
                Arc::new(output)
            }
        };
        Ok(ColumnarValue::Array(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, TimestampNanosecondArray};
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    fn evaluate(operation: Operation, args: Vec<ColumnarValue>, rows: usize) -> ArrayRef {
        function(operation)
            .invoke_with_args(ScalarFunctionArgs {
                args,
                number_rows: rows,
                return_field: Arc::new(Field::new("candidate", DataType::Int64, true)),
                config_options: Arc::new(ConfigOptions::default()),
                arg_fields: vec![],
            })
            .unwrap()
            .into_array(rows)
            .unwrap()
    }

    #[test]
    fn candidates_floor_negative_submillis_and_preserve_sliced_nulls() {
        let input = TimestampNanosecondArray::from(vec![Some(1), Some(-1), None, Some(1_999_999)])
            .slice(1, 3);
        let output = evaluate(
            Operation::Millis,
            vec![ColumnarValue::Array(Arc::new(input))],
            3,
        );
        assert_eq!(
            output.as_ref(),
            &Int64Array::from(vec![Some(-1), None, Some(1)])
        );
        output.to_data().validate_full().unwrap();
    }

    #[test]
    fn fixed_subtraction_wraps_like_flink() {
        let output = evaluate(
            Operation::SubtractMillis,
            vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                    Some(i64::MIN),
                    None,
                    Some(0),
                ]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ],
            3,
        );
        assert_eq!(
            output.as_ref(),
            &Int64Array::from(vec![Some(i64::MAX), None, Some(-1)])
        );
    }

    #[test]
    fn calendar_candidates_can_reverse_order_at_month_end() {
        let output = evaluate(
            Operation::SubtractMonths,
            vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                    1_711_839_600_000,
                    1_711_843_200_000,
                ]))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
            ],
            2,
        );
        // March 30 23:00 and March 31 00:00 clamp to February 29, retaining their times of day.
        assert_eq!(
            output.as_ref(),
            &Int64Array::from(vec![1_709_247_600_000, 1_709_164_800_000])
        );
    }

    #[test]
    fn timestamp_kernels_reject_wrong_types_and_arity() {
        for types in [
            vec![],
            vec![DataType::Utf8],
            vec![DataType::Int64, DataType::Int64],
        ] {
            assert!(function(Operation::Millis).return_type(&types).is_err());
        }
        assert!(function(Operation::SubtractMonths)
            .return_type(&[DataType::Int64, DataType::Int64])
            .is_err());
    }
}
