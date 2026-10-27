//! Preserve returned-array identity when a kernel copies elements into a GenericArrayData.
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int8Array, ListArray, StructArray};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn generic_elements(
    array: &ListArray,
    copied: impl Fn(usize) -> bool,
) -> Result<ArrayRef> {
    let Some(values) = array.values().as_any().downcast_ref::<StructArray>() else {
        return Ok(Arc::new(array.clone()));
    };
    let (index, mask) = if crate::flink_key::represented_string(values.data_type()) {
        if values.num_columns() < 3 {
            return Ok(Arc::new(array.clone()));
        }
        (2, 0)
    } else if let Some(index) = streamfusion_bridge::nested_row_kind_index(values.fields()) {
        // The row's own binary category and RowKind survive; only its containing array changes.
        (index, !8)
    } else {
        return Ok(Arc::new(array.clone()));
    };
    let original = values
        .column(index)
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    if !original.iter().flatten().any(|value| value & !mask != 0) {
        return Ok(Arc::new(array.clone()));
    }
    let mut flags = original.iter().collect::<Vec<_>>();
    let mut changed = false;
    for row in 0..array.len() {
        if array.is_valid(row) && copied(row) {
            for flag in &mut flags
                [array.value_offsets()[row] as usize..array.value_offsets()[row + 1] as usize]
            {
                if let Some(value) = flag {
                    let updated = *value & mask;
                    changed |= updated != *value;
                    *value = updated;
                }
            }
        }
    }
    if !changed {
        return Ok(Arc::new(array.clone()));
    }
    let mut columns = values.columns().to_vec();
    columns[index] = Arc::new(Int8Array::from(flags));
    let values = StructArray::try_new(values.fields().clone(), columns, values.nulls().cloned())?;
    let DataType::List(field) = array.data_type() else {
        unreachable!()
    };
    Ok(Arc::new(ListArray::try_new(
        field.clone(),
        array.offsets().clone(),
        Arc::new(values),
        array.nulls().cloned(),
    )?))
}

pub(super) fn reverse() -> ScalarUDF {
    ScalarUDF::new_from_impl(ArrayReverse {
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayReverse {
    signature: Signature,
}

impl ScalarUDFImpl for ArrayReverse {
    fn name(&self) -> &str {
        "flink_array_reverse"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [list @ DataType::List(_)] => Ok(list.clone()),
            _ => exec_err!("ARRAY_REVERSE requires an ARRAY"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args.first(), Some(ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        let result = datafusion::functions_nested::reverse::array_reverse_udf()
            .invoke_with_args(args)?
            .to_array(rows)?;
        let array = datafusion::common::cast::as_list_array(&result)?;
        let result = generic_elements(array, |row| array.value_length(row) > 1)?;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}
