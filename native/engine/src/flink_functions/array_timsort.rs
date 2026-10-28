use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, ListArray, UInt32Array};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_list_array, exec_err, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function(arity: usize) -> ScalarUDF {
    ScalarUDF::new_from_impl(ArraySort {
        signature: Signature::any(arity, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArraySort {
    signature: Signature,
}
impl ScalarUDFImpl for ArraySort {
    fn name(&self) -> &str {
        "flink_array_timsort"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [list @ DataType::List(field), options @ ..]
                if types.len() <= 3
                    && options.iter().all(|t| t == &DataType::Boolean)
                    && (matches!(
                        field.data_type(),
                        DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64
                            | DataType::Decimal128(_, _)
                    ) || crate::flink_key::represented_string(field.data_type())) =>
            {
                Ok(list.clone())
            }
            _ => exec_err!("ARRAY_SORT requires a verified ARRAY and Boolean options"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(sort, vec![])(&args.args)
    }
}

fn sort(args: &[ArrayRef]) -> Result<ArrayRef> {
    let array = as_list_array(&args[0])?;
    let ascending = args
        .get(1)
        .map(|a| datafusion::common::cast::as_boolean_array(a))
        .transpose()?;
    let null_first = args
        .get(2)
        .map(|a| datafusion::common::cast::as_boolean_array(a))
        .transpose()?;
    let values = array.values();
    let strings = crate::flink_key::represented_string(values.data_type())
        .then(|| super::collection_string::Strings::new(values))
        .transpose()?;
    let primitive = if strings.is_none() {
        Some(arrow_ord::ord::make_comparator(
            values.as_ref(),
            values.as_ref(),
            Default::default(),
        )?)
    } else {
        None
    };
    let mut selected = Vec::with_capacity(values.len());
    let mut offsets = vec![0];
    let mut valid = Vec::with_capacity(array.len());
    let mut indices = Vec::new();
    for row in 0..array.len() {
        let present = array.is_valid(row)
            && ascending.is_none_or(|a| a.is_valid(row))
            && null_first.is_none_or(|a| a.is_valid(row));
        valid.push(present);
        if present {
            let asc = ascending.is_none_or(|a| a.value(row));
            let first = null_first.map_or(asc, |a| a.value(row));
            indices.clear();
            indices.extend(
                array.value_offsets()[row] as usize..array.value_offsets()[row + 1] as usize,
            );
            let compare = |left: usize, right: usize| {
                match (values.is_null(left), values.is_null(right)) {
                    (true, true) => Ordering::Equal,
                    (true, false) => {
                        if first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                    (false, true) => {
                        if first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    (false, false) => {
                        let greater = if let Some(strings) = &strings {
                            strings.compare(left, strings, right).is_gt()
                        } else {
                            primitive.as_ref().unwrap()(left, right).is_gt()
                        };
                        // Released Flink intentionally returns no zero for equal non-NULL values.
                        if greater == asc {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                }
            };
            super::java_timsort::sort(&mut indices, compare).map_err(|_| {
                DataFusionError::External(Box::new(
                    streamfusion_bridge::FlinkException::illegal_argument(
                        "Comparison method violates its general contract!".into(),
                    ),
                ))
            })?;
            selected.extend(indices.iter().map(|&index| index as u32));
        }
        offsets.push(
            i32::try_from(selected.len()).map_err(|_| {
                DataFusionError::Execution("ARRAY_SORT exceeds Arrow offsets".into())
            })?,
        );
    }
    let DataType::List(field) = array.data_type() else {
        unreachable!()
    };
    let result = ListArray::try_new(
        field.clone(),
        OffsetBuffer::new(offsets.into()),
        arrow::compute::take(values.as_ref(), &UInt32Array::from(selected), None)?,
        Some(NullBuffer::from(valid)),
    )?;
    super::array_copy::generic_elements(&result, |_| true)
}
