//! Flink casts decimals through double precision before narrowing to FLOAT.

use arrow::array::{Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, ListArray};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

pub(crate) fn function(single_precision: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(DecimalFloat {
        single_precision,
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct DecimalFloat {
    single_precision: bool,
    signature: Signature,
}

impl DecimalFloat {
    fn output_type(&self, input: &DataType) -> Result<DataType> {
        match input {
            DataType::Decimal128(precision, scale)
                if (1..=38).contains(precision) && (0..=*precision as i8).contains(scale) =>
            {
                Ok(if self.single_precision {
                    DataType::Float32
                } else {
                    DataType::Float64
                })
            }
            DataType::List(field) if matches!(field.data_type(), DataType::Decimal128(..)) => {
                Ok(DataType::List(Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(self.output_type(field.data_type())?),
                )))
            }
            other => exec_err!("decimal floating cast does not support {other}"),
        }
    }

    fn convert(&self, input: &ArrayRef) -> Result<ArrayRef> {
        if let Some(list) = input.as_any().downcast_ref::<ListArray>() {
            let DataType::List(field) = self.output_type(input.data_type())? else {
                unreachable!()
            };
            return Ok(Arc::new(ListArray::try_new(
                field,
                list.offsets().clone(),
                self.convert(list.values())?,
                list.nulls().cloned(),
            )?));
        }
        self.output_type(input.data_type())?;
        let decimals = input.as_any().downcast_ref::<Decimal128Array>().unwrap();
        let factor = 10_u64
            .checked_pow(decimals.scale() as u32)
            .map(|n| n as f64);
        let double_value = |value: i128| {
            if decimals.precision() <= 18 {
                // Compact DecimalData divides its rounded long by an exact power of ten.
                value as f64 / factor.unwrap()
            } else {
                // Non-compact DecimalData delegates to BigDecimal.doubleValue(). Parse the
                // exact decimal into f64 before narrowing: direct f32 parsing can differ.
                format!("{value}e-{}", decimals.scale())
                    .parse::<f64>()
                    .expect("valid decimal coefficient and scale")
            }
        };
        if self.single_precision {
            Ok(Arc::new(
                decimals
                    .iter()
                    .map(|value| value.map(|value| double_value(value) as f32))
                    .collect::<Float32Array>(),
            ))
        } else {
            Ok(Arc::new(
                decimals
                    .iter()
                    .map(|value| value.map(double_value))
                    .collect::<Float64Array>(),
            ))
        }
    }
}

impl ScalarUDFImpl for DecimalFloat {
    fn name(&self) -> &str {
        if self.single_precision {
            "flink_decimal_to_float"
        } else {
            "flink_decimal_to_double"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [input] = args else {
            return exec_err!("decimal floating cast requires one argument");
        };
        self.output_type(input)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input] = args.args.as_slice() else {
            return exec_err!("decimal floating cast requires one argument");
        };
        let scalar = matches!(input, ColumnarValue::Scalar(_));
        let array = input.to_array(if scalar { 1 } else { args.number_rows })?;
        let output = self.convert(&array)?;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &output, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::Field;
    use datafusion::common::config::ConfigOptions;

    fn cast(input: ColumnarValue, single: bool, rows: usize) -> Result<ColumnarValue> {
        let function = function(single);
        let output = function.return_type(&[input.data_type()])?;
        function.invoke_with_args(ScalarFunctionArgs {
            args: vec![input],
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new("out", output, true)),
            config_options: Arc::new(ConfigOptions::new()),
        })
    }

    #[test]
    fn narrowing_keeps_the_intermediate_double_rounding() {
        // Just above the f32 midpoint, but its f64 value is exactly the midpoint.
        let value = 10000000596046447753906250000000000001_i128;
        let ColumnarValue::Scalar(actual) = cast(
            ColumnarValue::Scalar(ScalarValue::Decimal128(Some(value), 38, 37)),
            true,
            0,
        )
        .unwrap() else {
            panic!("expected scalar")
        };
        assert_eq!(actual, ScalarValue::Float32(Some(1.0)));
        assert_eq!(
            format!("{value}e-37").parse::<f32>().unwrap().to_bits(),
            1.0_f32.to_bits() + 1
        );
        let ColumnarValue::Scalar(null) = cast(
            ColumnarValue::Scalar(ScalarValue::Decimal128(None, 38, 37)),
            false,
            128,
        )
        .unwrap() else {
            panic!("expected scalar")
        };
        assert_eq!(null, ScalarValue::Float64(None));
    }

    #[test]
    fn sliced_lists_keep_offsets_container_nulls_and_element_nulls() {
        let values = Decimal128Array::from(vec![Some(9), Some(125), None, Some(-125), Some(7)])
            .with_precision_and_scale(18, 2)
            .unwrap();
        let lists = ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1, 4, 4, 5].into()),
            Arc::new(values),
            Some(NullBuffer::from(vec![true, true, true, false])),
        )
        .slice(1, 3);
        let ColumnarValue::Array(output) =
            cast(ColumnarValue::Array(Arc::new(lists)), true, 3).unwrap()
        else {
            panic!("expected array")
        };
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(output.value_offsets(), &[1, 4, 4, 5]);
        assert!(output.is_null(2));
        assert_eq!(output.value_length(1), 0);
        let first = output.value(0);
        assert_eq!(
            first.as_any().downcast_ref::<Float32Array>().unwrap(),
            &Float32Array::from(vec![Some(1.25), None, Some(-1.25)])
        );
    }

    #[test]
    fn empty_arrays_and_wrong_types_have_a_defined_contract() {
        let empty = Decimal128Array::from(Vec::<Option<i128>>::new())
            .with_precision_and_scale(38, 38)
            .unwrap();
        let ColumnarValue::Array(output) =
            cast(ColumnarValue::Array(Arc::new(empty)), false, 0).unwrap()
        else {
            panic!("expected array")
        };
        assert_eq!(output.len(), 0);
        assert_eq!(output.data_type(), &DataType::Float64);
        assert!(function(true).return_type(&[DataType::Int64]).is_err());
    }
}
