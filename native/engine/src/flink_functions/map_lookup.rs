use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Scalar, UInt32Array};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_map_array, Result, ScalarValue};
use datafusion::logical_expr::{ScalarUDF, Volatility};

pub(crate) fn function(map_type: DataType, key: ScalarValue) -> ScalarUDF {
    let DataType::Map(entries, _) = &map_type else {
        unreachable!("map lookup input")
    };
    let DataType::Struct(fields) = entries.data_type() else {
        unreachable!("map entries")
    };
    let output = fields[1].data_type().clone();
    datafusion::logical_expr::create_udf(
        &format!("flink_map_lookup_{key:?}"),
        vec![map_type],
        output,
        Volatility::Immutable,
        Arc::new(datafusion::functions::utils::make_scalar_function(
            move |args| lookup(&args[0], &key),
            vec![],
        )),
    )
}

fn lookup(input: &ArrayRef, key: &ScalarValue) -> Result<ArrayRef> {
    let map = as_map_array(input)?;
    let key = Scalar::new(key.to_array()?);
    let matches = arrow::compute::kernels::cmp::eq(map.keys(), &key)?;
    let indices: UInt32Array = (0..map.len())
        .map(|row| {
            if map.is_null(row) {
                return None;
            }
            let offsets = map.value_offsets();
            (offsets[row] as usize..offsets[row + 1] as usize)
                .find(|&entry| matches.is_valid(entry) && matches.value(entry))
                .map(|entry| entry as u32)
        })
        .collect();
    Ok(arrow::compute::take(map.values().as_ref(), &indices, None)?)
}
