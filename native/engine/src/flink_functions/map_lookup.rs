use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Scalar, UInt32Array};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_map_array, Result, ScalarValue};
use datafusion::logical_expr::{ScalarUDF, Volatility};

pub(crate) fn dynamic_function(map_type: DataType) -> ScalarUDF {
    let DataType::Map(entries, _) = &map_type else {
        unreachable!("map lookup input")
    };
    let DataType::Struct(fields) = entries.data_type() else {
        unreachable!("map entries")
    };
    let key_type = fields[0].data_type().clone();
    let output = fields[1].data_type().clone();
    datafusion::logical_expr::create_udf(
        "flink_dynamic_map_lookup",
        vec![map_type, key_type],
        output,
        Volatility::Immutable,
        Arc::new(datafusion::functions::utils::make_scalar_function(
            |args| dynamic_lookup(&args[0], &args[1]),
            vec![],
        )),
    )
}

fn dynamic_lookup(input: &ArrayRef, keys: &ArrayRef) -> Result<ArrayRef> {
    let map = as_map_array(input)?;
    let compare =
        arrow_ord::ord::make_comparator(map.keys().as_ref(), keys.as_ref(), Default::default())?;
    let offsets = map.value_offsets();
    let default = (map.keys().null_count() > 0)
        .then(|| null_key_read_value(map.keys().data_type()))
        .flatten()
        .map(|value| value.to_array())
        .transpose()?;
    let compare_default = default
        .as_ref()
        .map(|value| {
            arrow_ord::ord::make_comparator(value.as_ref(), keys.as_ref(), Default::default())
        })
        .transpose()?;
    let indices: UInt32Array = (0..map.len())
        .map(|row| {
            if map.is_null(row) || keys.is_null(row) {
                return None;
            }
            (offsets[row] as usize..offsets[row + 1] as usize)
                .find(|&entry| {
                    if map.keys().is_null(entry) {
                        compare_default
                            .as_ref()
                            .is_some_and(|compare| compare(0, row).is_eq())
                    } else {
                        compare(entry, row).is_eq()
                    }
                })
                .map(|entry| entry as u32)
        })
        .collect();
    Ok(arrow::compute::take(map.values().as_ref(), &indices, None)?)
}

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
    let matches_null_key = null_key_read_value(map.keys().data_type()).as_ref() == Some(key);
    let key = Scalar::new(key.to_array()?);
    let matches = arrow::compute::kernels::cmp::eq(map.keys(), &key)?;
    let indices: UInt32Array = (0..map.len())
        .map(|row| {
            if map.is_null(row) {
                return None;
            }
            let offsets = map.value_offsets();
            (offsets[row] as usize..offsets[row + 1] as usize)
                .find(|&entry| {
                    if map.keys().is_null(entry) {
                        matches_null_key
                    } else {
                        matches.is_valid(entry) && matches.value(entry)
                    }
                })
                .map(|entry| entry as u32)
        })
        .collect();
    Ok(arrow::compute::take(map.values().as_ref(), &indices, None)?)
}

// Flink's BinaryMap lookup reads these slots without checking the stored key's null bit.
fn null_key_read_value(data_type: &DataType) -> Option<ScalarValue> {
    match data_type {
        DataType::Utf8 => Some(ScalarValue::Utf8(Some(String::new()))),
        DataType::Date32 => Some(ScalarValue::Date32(Some(0))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, ListArray, MapArray, StringArray, StructArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Field, Int64Type};

    #[test]
    fn sliced_dynamic_map_uses_first_match_and_preserves_nested_values() {
        let keys: ArrayRef = Arc::new(StringArray::from(vec![
            Some("prefix"),
            None,
            Some("a"),
            Some("a"),
            Some("b"),
            Some("b"),
            Some("x"),
            Some("x"),
        ]));
        let values = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(0)]),
            Some(vec![Some(99)]),
            Some(vec![Some(10), None]),
            Some(vec![Some(20)]),
            None,
            Some(vec![]),
            Some(vec![Some(30)]),
            Some(vec![Some(40)]),
        ]);
        let entries = StructArray::from(vec![
            (Arc::new(Field::new("key", DataType::Utf8, true)), keys),
            (
                Arc::new(Field::new("value", values.data_type().clone(), true)),
                Arc::new(values.clone()) as ArrayRef,
            ),
        ]);
        let map: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 1, 4, 5, 6, 7, 8, 8].into()),
            entries,
            Some(NullBuffer::from(vec![
                true, true, true, true, false, true, true,
            ])),
            false,
        ));
        let lookup_keys: ArrayRef = Arc::new(StringArray::from(vec![
            Some("prefix"),
            Some("a"),
            Some("b"),
            Some("b"),
            Some("x"),
            None,
            Some("absent"),
        ]));
        let result = dynamic_lookup(&map.slice(1, 6), &lookup_keys.slice(1, 6)).unwrap();
        let expected = arrow::compute::take(
            &values,
            &UInt32Array::from(vec![Some(2), Some(4), Some(5), None, None, None]),
            None,
        )
        .unwrap();
        assert_eq!(result.as_ref(), expected.as_ref());
        assert_eq!(
            dynamic_lookup(&map.slice(0, 0), &lookup_keys.slice(0, 0))
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn null_lookup_key_stays_null_with_null_stored_key() {
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![None, Some("a")])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Int64, true)),
                Arc::new(Int64Array::from(vec![99, 7])) as ArrayRef,
            ),
        ]);
        let input: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 2].into()),
            entries,
            None,
            false,
        ));
        for (key, expected) in [(None, None), (Some("a"), Some(7)), (Some("missing"), None)] {
            let key: ArrayRef = Arc::new(StringArray::from(vec![key]));
            let result = dynamic_lookup(&input, &key).unwrap();
            assert_eq!(result.as_ref(), &Int64Array::from(vec![expected]));
        }
    }
}
