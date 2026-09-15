use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type};
use arrow::error::ArrowError;
use datafusion::common::{cast::as_primitive_array, exec_err, Result};
use datafusion::logical_expr::ScalarUDF;

pub(crate) fn function(types: &[DataType]) -> Option<ScalarUDF> {
    if types.len() != 2
        || !types
            .iter()
            .all(|t| t.is_signed_integer() || *t == DataType::Null)
    {
        return None;
    }
    let datatype = [
        DataType::Int64,
        DataType::Int32,
        DataType::Int16,
        DataType::Int8,
    ]
    .into_iter()
    .find(|t| types.contains(t))?;
    Some(super::udf(
        "flink_integer_divide",
        vec![datatype.clone(); 2],
        datatype,
        divide,
    ))
}

fn divide(args: &[ArrayRef]) -> Result<ArrayRef> {
    macro_rules! divide {
        ($t:ty) => {{
            let left = as_primitive_array::<$t>(&args[0])?;
            let right = as_primitive_array::<$t>(&args[1])?;
            Arc::new(arrow::compute::kernels::arity::try_binary::<_, _, _, $t>(
                left,
                right,
                |a, b| {
                    if b == 0 {
                        Err(ArrowError::DivideByZero)
                    } else {
                        Ok(a.wrapping_div(b))
                    }
                },
            )?)
        }};
    }
    Ok(match args[0].data_type() {
        DataType::Int8 => divide!(Int8Type),
        DataType::Int16 => divide!(Int16Type),
        DataType::Int32 => divide!(Int32Type),
        DataType::Int64 => divide!(Int64Type),
        other => return exec_err!("integer division input {other}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    #[test]
    fn wraps_only_overflow_and_preserves_null_and_zero_errors() {
        let left: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            Some(-7),
            None,
            Some(3),
        ]));
        let right: ArrayRef = Arc::new(Int64Array::from(vec![Some(-1), Some(2), Some(0), None]));
        let result = divide(&[left, right]).unwrap();
        assert_eq!(
            as_primitive_array::<Int64Type>(&result).unwrap(),
            &Int64Array::from(vec![Some(i64::MIN), Some(-3), None, None])
        );
        assert!(divide(&[
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![0]))
        ])
        .is_err());
    }
}
