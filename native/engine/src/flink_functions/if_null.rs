use arrow::array::{Array, ArrayRef};
use arrow::compute::{is_null, kernels::zip::zip};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(IfNull {
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct IfNull {
    signature: Signature,
}

impl ScalarUDFImpl for IfNull {
    fn name(&self) -> &str {
        "flink_if_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(vec![self.return_type(types)?; 2])
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::Null, other] | [other, DataType::Null] => Ok(other.clone()),
            [left, right] if left == right => Ok(left.clone()),
            _ => exec_err!("IFNULL requires two operands with Flink's resolved common type"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Flink evaluates both arguments before IfNullFunction.eval. A conditional DataFusion
        // COALESCE/CASE would suppress errors and could evaluate a volatile first argument twice.
        datafusion::functions::utils::make_scalar_function(if_null, vec![])(&args.args)
    }
}

fn if_null(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input, replacement] = args else {
        return exec_err!("IFNULL expects two arguments");
    };
    if input.data_type() != replacement.data_type() || input.len() != replacement.len() {
        return exec_err!("IFNULL arguments must have equal types and lengths");
    }
    let null_count = input.logical_null_count();
    if null_count == 0 {
        return Ok(input.clone());
    }
    if null_count == input.len() {
        return Ok(replacement.clone());
    }
    Ok(zip(&is_null(input.as_ref())?, replacement, input)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal128Array, Int32Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn sliced_validity_selects_values_without_losing_decimal_metadata() {
        let input: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(9), None, Some(-1234), None])
                .with_precision_and_scale(12, 3)
                .unwrap(),
        );
        let replacement: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(7), Some(5555), None, None])
                .with_precision_and_scale(12, 3)
                .unwrap(),
        );
        let result = if_null(&[input.slice(1, 3), replacement.slice(1, 3)]).unwrap();
        let expected: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(5555), Some(-1234), None])
                .with_precision_and_scale(12, 3)
                .unwrap(),
        );
        assert_eq!(&result, &expected);
    }

    #[test]
    fn empty_strings_are_values_and_all_nulls_use_the_replacement() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![Some(""), None, Some("a\0b")]));
        let replacement: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), None, Some("y")]));
        let expected: ArrayRef = Arc::new(StringArray::from(vec![Some(""), None, Some("a\0b")]));
        assert_eq!(&if_null(&[input, replacement.clone()]).unwrap(), &expected);
        let nulls: ArrayRef = Arc::new(StringArray::new_null(3));
        assert!(Arc::ptr_eq(
            &if_null(&[nulls, replacement.clone()]).unwrap(),
            &replacement
        ));
    }

    #[test]
    fn no_nulls_and_empty_batches_reuse_the_input() {
        for values in [vec![], vec![1, 2, 3]] {
            let input: ArrayRef = Arc::new(Int32Array::from(values));
            let replacement: ArrayRef = Arc::new(Int32Array::new_null(input.len()));
            assert!(Arc::ptr_eq(
                &if_null(&[input.clone(), replacement]).unwrap(),
                &input
            ));
        }
    }
}
