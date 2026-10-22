//! Nullable exact-numeric arrays reuse Flink's scalar conversions over their flat children.

use arrow::array::{Array, ArrayRef, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use datafusion::common::{config::ConfigOptions, exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

pub(crate) fn function(target: DataType) -> ScalarUDF {
    ScalarUDF::new_from_impl(ArrayCast {
        target,
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayCast {
    target: DataType,
    signature: Signature,
}

fn exact_numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    ) || matches!(data_type, DataType::Decimal128(p, s)
            if (1..=38).contains(p) && (0..=*p as i8).contains(s))
}

fn validate(source: &DataType, target: &DataType) -> Result<()> {
    match (source, target) {
        (DataType::List(from), DataType::List(to)) if from.is_nullable() && to.is_nullable() => {
            validate(from.data_type(), to.data_type())
        }
        _ if exact_numeric(source) && exact_numeric(target) => Ok(()),
        _ => exec_err!("unsupported exact ARRAY cast: {source} to {target}"),
    }
}

fn convert(input: &ArrayRef, target: &DataType, config: &Arc<ConfigOptions>) -> Result<ArrayRef> {
    if let (Some(input), DataType::List(field)) =
        (input.as_any().downcast_ref::<ListArray>(), target)
    {
        let offsets = input.value_offsets();
        let start = offsets[0] as usize;
        let end = offsets[offsets.len() - 1] as usize;
        // Slices retain the original child buffer; convert only this batch's element range.
        let values = input.values().slice(start, end - start);
        let offsets = if start == 0 {
            input.offsets().clone()
        } else {
            OffsetBuffer::new(offsets.iter().map(|offset| offset - start as i32).collect())
        };
        return Ok(Arc::new(ListArray::try_new(
            field.clone(),
            offsets,
            convert(&values, field.data_type(), config)?,
            input.nulls().cloned(),
        )?));
    }
    if input.data_type() == target {
        return Ok(input.clone());
    }
    if input.data_type().is_integer()
        && target.is_integer()
        && input.data_type().primitive_width() <= target.primitive_width()
    {
        return Ok(arrow::compute::cast(input, target)?);
    }
    let function: Box<dyn ScalarUDFImpl> = match target {
        DataType::Decimal128(p, s) => Box::new(super::decimal::DecimalCast::new(*p, *s)),
        _ => Box::new(crate::expr::NarrowingCast::new(target.clone())),
    };
    function
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input.clone())],
            arg_fields: vec![Arc::new(Field::new(
                "input",
                input.data_type().clone(),
                true,
            ))],
            number_rows: input.len(),
            return_field: Arc::new(Field::new("output", target.clone(), true)),
            config_options: config.clone(),
        })?
        .to_array(input.len())
}

impl ScalarUDFImpl for ArrayCast {
    fn name(&self) -> &str {
        "flink_exact_array_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [source @ DataType::List(_)] = args else {
            return exec_err!("exact ARRAY cast requires one ARRAY argument");
        };
        validate(source, &self.target)?;
        Ok(self.target.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input] = args.args.as_slice() else {
            return exec_err!("exact ARRAY cast requires one argument");
        };
        self.return_type(&[input.data_type()])?;
        let scalar = matches!(input, ColumnarValue::Scalar(_));
        let input = input.to_array(if scalar { 1 } else { args.number_rows })?;
        let output = convert(&input, &self.target, &args.config_options)?;
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
    use arrow::array::{Decimal128Array, Int32Array, Int64Array, Int8Array};
    use arrow::buffer::{NullBuffer, OffsetBuffer};

    fn list(values: ArrayRef, offsets: Vec<i32>, valid: Vec<bool>) -> ArrayRef {
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(offsets.into()),
            values,
            Some(NullBuffer::from(valid)),
        ))
    }

    fn cast(input: ArrayRef, leaf: DataType) -> ArrayRef {
        convert(
            &input,
            &DataType::List(Arc::new(Field::new("item", leaf, true))),
            &Arc::new(ConfigOptions::new()),
        )
        .unwrap()
    }

    #[test]
    fn sliced_lists_keep_offsets_null_containers_and_elements() {
        let input = list(
            Arc::new(Int32Array::from(vec![Some(99), Some(1), None, Some(-2)])),
            vec![0, 1, 4, 4, 4],
            vec![true, true, true, false],
        )
        .slice(1, 3);
        let result = cast(input, DataType::Int64);
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(result.value_offsets(), &[0, 3, 3, 3]);
        assert_eq!(result.values().len(), 3);
        assert_eq!(
            result.value(0).as_ref(),
            &Int64Array::from(vec![Some(1), None, Some(-2)])
        );
        assert_eq!(result.value_length(1), 0);
        assert!(result.is_null(2));
    }

    #[test]
    fn decimal_rounding_overflow_and_integer_wrap_match_scalar_rules() {
        let input = list(
            Arc::new(
                Decimal128Array::from(vec![Some(125), Some(-255), Some(99999), None])
                    .with_precision_and_scale(5, 2)
                    .unwrap(),
            ),
            vec![0, 4],
            vec![true],
        );
        let rounded = cast(input.clone(), DataType::Decimal128(3, 1));
        assert_eq!(
            rounded
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(0)
                .as_ref(),
            &Decimal128Array::from(vec![Some(13), Some(-26), None, None])
                .with_precision_and_scale(3, 1)
                .unwrap()
        );
        let narrowed = cast(input, DataType::Int8);
        assert_eq!(
            narrowed
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(0)
                .as_ref(),
            &Int8Array::from(vec![Some(1), Some(-2), Some(999_i32 as i8), None])
        );
    }

    #[test]
    fn hidden_null_children_and_empty_batches_do_not_fail() {
        let input = list(
            Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX])),
            vec![0, 2, 2],
            vec![false, true],
        );
        let output = cast(input, DataType::Decimal128(1, 0));
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(output.is_null(0));
        assert_eq!(output.value_length(1), 0);
        assert_eq!(cast(Arc::new(output.slice(0, 0)), DataType::Int32).len(), 0);
        let nonnullable = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
        assert!(function(nonnullable.clone())
            .return_type(&[nonnullable])
            .is_err());
    }
}
