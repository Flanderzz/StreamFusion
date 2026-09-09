use crate::HashMap;
use arrow::array::{Array, ArrayRef, BooleanArray, PrimitiveArray, StringArray, StringBuilder};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal128Type, Int16Type, Int32Type, Int64Type, Int8Type,
};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

pub(super) fn extremum(greatest: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(FlinkExtremum {
        greatest,
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FlinkExtremum {
    greatest: bool,
    signature: Signature,
}

impl FlinkExtremum {
    fn delegate(&self) -> Arc<ScalarUDF> {
        if self.greatest {
            datafusion::functions::core::greatest()
        } else {
            datafusion::functions::core::least()
        }
    }
}

impl ScalarUDFImpl for FlinkExtremum {
    fn name(&self) -> &str {
        if self.greatest {
            "flink_greatest"
        } else {
            "flink_least"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if types.len() < 2 {
            return exec_err!("GREATEST/LEAST require at least two arguments");
        }
        self.delegate().return_type(types)
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        self.return_type(types)?;
        self.delegate().coerce_types(types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 {
            return exec_err!("GREATEST/LEAST require at least two arguments");
        }
        for arg in &args.args {
            if let ColumnarValue::Array(array) = arg {
                if array.len() != args.number_rows {
                    return exec_err!("GREATEST/LEAST array length differs from batch length");
                }
            }
        }
        if args
            .args
            .iter()
            .any(|arg| matches!(arg, ColumnarValue::Scalar(s) if s.is_null()))
        {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(
                args.return_type(),
            )?));
        }
        if args
            .args
            .iter()
            .any(|arg| matches!(arg, ColumnarValue::Array(_)))
        {
            let result = match args.return_type() {
                DataType::Int8 => Some(primitive_extremum::<Int8Type>(&args.args, self.greatest)?),
                DataType::Int16 => {
                    Some(primitive_extremum::<Int16Type>(&args.args, self.greatest)?)
                }
                DataType::Int32 => {
                    Some(primitive_extremum::<Int32Type>(&args.args, self.greatest)?)
                }
                DataType::Int64 => {
                    Some(primitive_extremum::<Int64Type>(&args.args, self.greatest)?)
                }
                DataType::Decimal128(_, _) => Some(primitive_extremum::<Decimal128Type>(
                    &args.args,
                    self.greatest,
                )?),
                _ => None,
            };
            if let Some(result) = result {
                return Ok(ColumnarValue::Array(result));
            }
        }
        let mut nulls = None;
        for arg in &args.args {
            if let ColumnarValue::Array(array) = arg {
                nulls = NullBuffer::union(nulls.as_ref(), array.nulls());
            }
        }
        let result = self.delegate().invoke_with_args(args)?;
        match (result, nulls) {
            (ColumnarValue::Array(array), Some(valid)) if valid.null_count() > 0 => {
                // Flink propagates any input NULL; DataFusion otherwise skips it. nullif reuses
                // the value buffers without another validation pass over string payloads.
                let mask = BooleanArray::new(!valid.inner(), None);
                Ok(ColumnarValue::Array(arrow::compute::nullif(
                    array.as_ref(),
                    &mask,
                )?))
            }
            (result, _) => Ok(result),
        }
    }
}

fn primitive_extremum<T: ArrowPrimitiveType>(
    args: &[ColumnarValue],
    greatest: bool,
) -> Result<ArrayRef>
where
    T::Native: Ord,
{
    use arrow::compute::kernels::arity::binary;
    let choose = |a: T::Native, b: T::Native| if greatest { a.max(b) } else { a.min(b) };
    let mut scalar = None;
    let mut arrays = Vec::new();
    for arg in args {
        match arg {
            ColumnarValue::Scalar(value) => {
                let array = value.to_array_of_size(1)?;
                let value = datafusion::common::cast::as_primitive_array::<T>(&array)?.value(0);
                scalar = Some(scalar.map_or(value, |previous| choose(previous, value)));
            }
            ColumnarValue::Array(array) => {
                arrays.push(datafusion::common::cast::as_primitive_array::<T>(array)?);
            }
        }
    }
    let Some((first, rest)) = arrays.split_first() else {
        return exec_err!("Primitive extremum requires an array");
    };
    let with_scalar = |value| scalar.map_or(value, |s| choose(value, s));
    let (mut result, remaining): (PrimitiveArray<T>, _) = match rest.split_first() {
        Some((second, remaining)) => (
            binary(first, second, |a, b| with_scalar(choose(a, b)))?,
            remaining,
        ),
        None => (first.unary(with_scalar), rest),
    };
    for array in remaining {
        result = binary(&result, array, choose)?;
    }
    // Arrow's generic arithmetic constructors use a default decimal scale; retain the coerced type.
    Ok(Arc::new(result.with_data_type(first.data_type().clone())))
}

pub(super) fn initcap(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [arg] = args else {
        return exec_err!("INITCAP expects one argument");
    };
    let strings = datafusion::common::cast::as_string_array(arg)?;
    let mut builder = StringBuilder::with_capacity(strings.len(), string_bytes(strings));
    let mut output = Vec::new();
    for value in strings {
        let Some(value) = value else {
            builder.append_null();
            continue;
        };
        output.clear();
        let mut start = true;
        for byte in value.bytes() {
            output.push(if start {
                byte.to_ascii_uppercase()
            } else {
                byte.to_ascii_lowercase()
            });
            start = !byte.is_ascii_alphanumeric();
        }
        // Only ASCII case bits changed; non-ASCII bytes retain their original UTF-8 encoding.
        builder.append_value(
            std::str::from_utf8(&output)
                .map_err(|e| datafusion::common::exec_datafusion_err!("INITCAP: {e}"))?,
        );
    }
    Ok(Arc::new(builder.finish()))
}

fn string_bytes(strings: &StringArray) -> usize {
    (strings.value_offsets()[strings.len()] - strings.value_offsets()[0]) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;

    fn strings(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(values))
    }

    fn values(array: &ArrayRef) -> Vec<Option<&str>> {
        datafusion::common::cast::as_string_array(array)
            .unwrap()
            .iter()
            .collect()
    }

    #[test]
    fn extrema_reject_bad_arity_and_array_lengths() {
        for op in [109, 110] {
            let udf = super::super::function(op, 2).unwrap();
            assert!(udf.return_type(&[]).is_err());
            assert!(udf.coerce_types(&[DataType::Int32]).is_err());
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(strings(vec![Some("a")])),
                    ColumnarValue::Array(strings(vec![Some("a"), None])),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(arrow::datatypes::Field::new("out", DataType::Utf8, true)),
                config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
            };
            assert!(udf.invoke_with_args(args).is_err());
        }
    }

    fn invoke(op: i64, args: Vec<ColumnarValue>, datatype: DataType, rows: usize) -> ColumnarValue {
        super::super::function(op, args.len())
            .unwrap()
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: rows,
                return_field: Arc::new(arrow::datatypes::Field::new("out", datatype, true)),
                config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
            })
            .unwrap()
    }

    #[test]
    fn primitive_extrema_preserve_sliced_nulls_and_decimal_scale() {
        use arrow::array::Int64Array;
        for datatype in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Decimal128(20, 3),
        ] {
            let first = Int64Array::from(vec![Some(0), Some(-9), None, Some(5), Some(12), Some(7)]);
            let second =
                Int64Array::from(vec![Some(0), Some(-2), Some(3), None, Some(2), Some(11)]);
            let first = arrow::compute::cast(&first.slice(1, 5), &datatype).unwrap();
            let second = arrow::compute::cast(&second.slice(1, 5), &datatype).unwrap();
            let scalar = ScalarValue::Int64(Some(4)).cast_to(&datatype).unwrap();
            for (op, expected) in [
                (109, vec![Some(4), None, None, Some(12), Some(11)]),
                (110, vec![Some(-9), None, None, Some(2), Some(4)]),
            ] {
                let args = vec![
                    ColumnarValue::Array(first.clone()),
                    ColumnarValue::Scalar(scalar.clone()),
                    ColumnarValue::Array(second.clone()),
                    ColumnarValue::Array(first.clone()),
                ];
                let actual = invoke(op, args, datatype.clone(), 5).into_array(5).unwrap();
                let expected =
                    arrow::compute::cast(&Int64Array::from(expected), &datatype).unwrap();
                assert_eq!(actual.to_data(), expected.to_data());
                let args = vec![
                    ColumnarValue::Array(first.slice(0, 0)),
                    ColumnarValue::Scalar(scalar.clone()),
                ];
                let empty = invoke(op, args, datatype.clone(), 0).into_array(0).unwrap();
                assert_eq!(empty.len(), 0);
                assert_eq!(empty.data_type(), &datatype);
                let args = vec![
                    ColumnarValue::Array(first.clone()),
                    ColumnarValue::Scalar(ScalarValue::try_new_null(&datatype).unwrap()),
                ];
                assert_eq!(
                    invoke(op, args, datatype.clone(), 5)
                        .into_array(5)
                        .unwrap()
                        .null_count(),
                    5
                );
            }
        }
    }
}
