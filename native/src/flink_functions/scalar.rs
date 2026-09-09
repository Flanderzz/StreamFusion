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
}
