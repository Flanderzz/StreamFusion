use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Float32Type, Float64Type, Int32Type, Int64Type};
use datafusion::common::{cast::as_primitive_array, exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(FlinkSign {
        signature: Signature::uniform(
            1,
            vec![
                DataType::Float64,
                DataType::Float32,
                DataType::Int32,
                DataType::Int64,
            ],
            Volatility::Immutable,
        ),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FlinkSign {
    signature: Signature,
}

impl ScalarUDFImpl for FlinkSign {
    fn name(&self) -> &str {
        "flink_sign"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [t @ (DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64)] => {
                Ok(t.clone())
            }
            _ => exec_err!("SIGN requires one signed integer or floating operand"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke(&args.args)
    }
}

fn invoke(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [input] = args else {
        return exec_err!("SIGN requires one operand");
    };
    match input {
        ColumnarValue::Array(array)
            if matches!(array.data_type(), DataType::Int32 | DataType::Int64) =>
        {
            Ok(ColumnarValue::Array(sign(std::slice::from_ref(array))?))
        }
        ColumnarValue::Scalar(ScalarValue::Int32(v)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Int32(v.map(i32::signum)),
        )),
        ColumnarValue::Scalar(ScalarValue::Int64(v)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Int64(v.map(i64::signum)),
        )),
        _ => datafusion::functions::utils::make_scalar_function(sign, vec![])(args),
    }
}

fn sign(args: &[ArrayRef]) -> Result<ArrayRef> {
    macro_rules! integer {
        ($t:ty) => {
            Arc::new(as_primitive_array::<$t>(&args[0])?.unary::<_, $t>(|v| v.signum()))
        };
    }
    macro_rules! sign {
        ($t:ty) => {
            Arc::new(as_primitive_array::<$t>(&args[0])?.unary::<_, $t>(|value| {
                if value == 0.0 || value.is_nan() {
                    value
                } else {
                    value.signum()
                }
            }))
        };
    }
    Ok(match args[0].data_type() {
        DataType::Float32 => sign!(Float32Type),
        DataType::Float64 => sign!(Float64Type),
        DataType::Int32 => integer!(Int32Type),
        DataType::Int64 => integer!(Int64Type),
        other => return exec_err!("SIGN expected signed integer or floating input, got {other}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, Int64Array};

    #[test]
    fn integer_widths_minima_nulls_and_sliced_validity() {
        macro_rules! check {
            ($array:ty, $native:ty) => {{
                let input = <$array>::from(vec![
                    Some(42),
                    Some(<$native>::MIN),
                    Some(-1),
                    None,
                    Some(0),
                    Some(1),
                    Some(<$native>::MAX),
                    Some(-42),
                ])
                .slice(1, 6);
                let output = sign(&[Arc::new(input.clone())]).unwrap();
                let output = output.as_any().downcast_ref::<$array>().unwrap();
                assert_eq!(output.data_type(), input.data_type());
                assert_eq!(output.nulls(), input.nulls());
                assert_eq!(
                    output.iter().collect::<Vec<_>>(),
                    vec![Some(-1), Some(-1), None, Some(0), Some(1), Some(1)]
                );
                assert_eq!(input.value(0), <$native>::MIN);
                for empty in [input.slice(3, 0), <$array>::from(vec![None; 3])] {
                    let result = sign(&[Arc::new(empty.clone())]).unwrap();
                    assert_eq!(result.len(), empty.len());
                    assert_eq!(result.null_count(), empty.null_count());
                }
            }};
        }
        check!(Int32Array, i32);
        check!(Int64Array, i64);
    }

    #[test]
    fn integer_scalars_keep_their_declared_width() {
        macro_rules! check {
            ($variant:ident, $native:ty) => {
                for (input, expected) in [
                    (Some(<$native>::MIN), Some(-1)),
                    (Some(-1), Some(-1)),
                    (Some(0), Some(0)),
                    (Some(<$native>::MAX), Some(1)),
                    (None, None),
                ] {
                    let output =
                        invoke(&[ColumnarValue::Scalar(ScalarValue::$variant(input))]).unwrap();
                    let ColumnarValue::Scalar(output) = output else {
                        panic!("Scalar SIGN returned an array");
                    };
                    assert_eq!(output, ScalarValue::$variant(expected));
                }
            };
        }
        check!(Int32, i32);
        check!(Int64, i64);
    }

    #[test]
    fn rejects_unsupported_types_and_arities() {
        let f = function();
        for types in [
            vec![],
            vec![DataType::UInt32],
            vec![DataType::Decimal128(18, 3)],
            vec![DataType::Int32, DataType::Int64],
        ] {
            assert!(f.return_type(&types).is_err());
        }
        assert!(invoke(&[]).is_err());
    }
}
