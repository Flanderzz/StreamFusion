use arrow::array::{ArrayRef, Float32Array, Float64Array};
use arrow::datatypes::DataType;
use std::sync::Arc;

// Java Float/Double.compare canonicalize every NaN payload but distinguish signed zeros.
pub fn canonical_ordering_column(column: &ArrayRef) -> ArrayRef {
    match column.data_type() {
        DataType::Float32 => Arc::new(Float32Array::from_iter(
            column
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .iter()
                .map(|v| v.map(|v| if v.is_nan() { f32::NAN } else { v })),
        )),
        DataType::Float64 => Arc::new(Float64Array::from_iter(
            column
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .map(|v| v.map(|v| if v.is_nan() { f64::NAN } else { v })),
        )),
        _ => column.clone(),
    }
}
