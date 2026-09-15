//! Java boxed floating equality canonicalizes NaN payloads but preserves signed zero.
//! Scalar grouping keys intentionally keep their raw bits instead.

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use datafusion::common::ScalarValue;
use std::sync::Arc;

pub(crate) fn canonical_f32(value: f32) -> f32 {
    if value.is_nan() {
        f32::from_bits(0x7fc00000)
    } else {
        value
    }
}

pub(crate) fn canonical_f64(value: f64) -> f64 {
    if value.is_nan() {
        f64::from_bits(0x7ff8000000000000)
    } else {
        value
    }
}

pub(crate) fn canonical_scalar(value: &ScalarValue) -> Option<ScalarValue> {
    match value {
        ScalarValue::Float32(Some(v)) if v.is_nan() => {
            Some(ScalarValue::Float32(Some(canonical_f32(*v))))
        }
        ScalarValue::Float64(Some(v)) if v.is_nan() => {
            Some(ScalarValue::Float64(Some(canonical_f64(*v))))
        }
        _ => None,
    }
}

pub(crate) fn canonical_array(array: &ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Float32 => Arc::new(
            array
                .as_primitive::<Float32Type>()
                .unary::<_, Float32Type>(canonical_f32),
        ),
        DataType::Float64 => Arc::new(
            array
                .as_primitive::<Float64Type>()
                .unary::<_, Float64Type>(canonical_f64),
        ),
        _ => array.clone(),
    }
}
