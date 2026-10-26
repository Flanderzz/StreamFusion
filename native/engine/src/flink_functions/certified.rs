//! Batch certificates for builtins with a retained released-Flink evaluator.
//! A failed certificate never returns a partial batch or guesses at Java behavior.

use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use std::sync::Arc;
use streamfusion_bridge::string_cache::JavaStringCacheColumn;

mod patterns;
mod printf;
mod url;

pub(crate) fn evaluate(
    operation: i64,
    arrays: &[ArrayRef],
    caches: &[ArrayRef],
    target: &DataType,
) -> Option<ArrayRef> {
    if operation == 12 {
        let [source, bits] = arrays else {
            return None;
        };
        return super::sha2_runtime::evaluate([source, bits]);
    }
    if operation == 11 {
        let [input, separator, index] = arrays else {
            return None;
        };
        return super::split_index::integer_separator([input, separator, index], caches);
    }
    for (index, input) in arrays.iter().enumerate() {
        if let Some(strings) = input.as_any().downcast_ref::<StringArray>() {
            let cache = caches
                .get(index)
                .filter(|a| a.data_type() != &DataType::Null)
                .map(JavaStringCacheColumn::new);
            for (row, value) in strings.iter().enumerate() {
                let Some(value) = value else { continue };
                if !value.is_ascii() {
                    return None;
                }
                if let Some(units) = cache.as_ref().and_then(|cache| cache.value(row)) {
                    if !units.iter().copied().eq(value.bytes().map(u16::from)) {
                        return None;
                    }
                }
            }
        }
    }
    match operation {
        1..=8 => patterns::evaluate(operation, arrays, target),
        9 => url::evaluate(arrays),
        10 => printf::evaluate(arrays),
        _ => None,
    }
}

fn text(array: &ArrayRef, row: usize) -> Option<&str> {
    let values = array.as_any().downcast_ref::<StringArray>()?;
    (!values.is_null(row)).then(|| values.value(row))
}

fn integer(array: &ArrayRef, row: usize) -> Option<i64> {
    if array.is_null(row) {
        return None;
    }
    macro_rules! primitive {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .map(|a| i64::from(a.value(row)))
        };
    }
    match array.data_type() {
        DataType::Int64 => primitive!(Int64Array),
        DataType::Int32 => primitive!(Int32Array),
        DataType::Int16 => primitive!(arrow::array::Int16Array),
        DataType::Int8 => primitive!(arrow::array::Int8Array),
        _ => None,
    }
}

fn number_output(values: Vec<Option<i64>>, target: &DataType) -> Option<ArrayRef> {
    Some(match target {
        DataType::Int32 => Arc::new(Int32Array::from_iter(
            values.into_iter().map(|v| v.map(|v| v as i32)),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(values)),
        DataType::Boolean => Arc::new(BooleanArray::from_iter(
            values.into_iter().map(|v| v.map(|v| v != 0)),
        )),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_java_utf16_must_agree_before_native_matching() {
        let args = [
            Arc::new(StringArray::from(vec!["?"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["."])) as ArrayRef,
        ];
        let cache = streamfusion_bridge::string_cache::string_cache_array([Some(&[0xd800][..])]);
        assert!(evaluate(1, &args, &[cache], &DataType::Boolean).is_none());
        assert!(evaluate(1, &args, &[], &DataType::Boolean).is_some());
    }
}
