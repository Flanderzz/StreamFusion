use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt32Array};
use arrow::datatypes::DataType;
use datafusion::common::{
    cast::{as_int64_array, as_list_array},
    Result,
};
use datafusion::logical_expr::{ScalarUDF, Volatility};

pub(crate) fn function(array_type: DataType) -> ScalarUDF {
    let DataType::List(element) = &array_type else {
        unreachable!("array subscript input")
    };
    let output = element.data_type().clone();
    datafusion::logical_expr::create_udf(
        "flink_array_item",
        vec![array_type, DataType::Int64],
        output,
        Volatility::Immutable,
        Arc::new(datafusion::functions::utils::make_scalar_function(
            |args| lookup(&args[0], &args[1]),
            vec![],
        )),
    )
}

fn lookup(input: &ArrayRef, indexes: &ArrayRef) -> Result<ArrayRef> {
    let array = as_list_array(input)?;
    let indexes = as_int64_array(indexes)?;
    let offsets = array.value_offsets();
    let indices: UInt32Array = (0..array.len())
        .map(|row| {
            if array.is_null(row) || indexes.is_null(row) {
                return None;
            }
            let index = indexes.value(row);
            let start = offsets[row] as i64;
            let length = (offsets[row + 1] as i64) - start;
            // Flink indexes from one; runtime zero/negative indexes never count from the end.
            (index >= 1 && index <= length).then(|| (start + index - 1) as u32)
        })
        .collect();
    Ok(arrow::compute::take(
        array.values().as_ref(),
        &indices,
        None,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, ListArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::Field;

    #[test]
    fn sliced_array_preserves_nulls_and_rejects_nonpositive_indexes() {
        let offsets = OffsetBuffer::new(vec![0, 2, 5, 8, 11, 14, 17, 20, 23, 26].into());
        let values = Arc::new(Int64Array::from(
            (0..26).map(|n| (n != 6).then_some(n)).collect::<Vec<_>>(),
        ));
        let array: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            offsets,
            values,
            Some(NullBuffer::from(vec![
                true, true, true, true, true, true, true, false, true,
            ])),
        ));
        let indexes: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(99),
            Some(3),
            Some(2),
            Some(0),
            Some(-1),
            Some(i64::MIN),
            Some(i64::MAX),
            Some(1),
            None,
        ]));
        let result = lookup(&array.slice(1, 8), &indexes.slice(1, 8)).unwrap();
        assert_eq!(
            result.as_ref(),
            &Int64Array::from(vec![Some(4), None, None, None, None, None, None, None])
        );
        assert_eq!(
            lookup(&array.slice(0, 0), &indexes.slice(0, 0))
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn array_item_retains_nested_schema_and_values() {
        let inner = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(vec![
            Some(vec![Some(7), None]),
            None,
            Some(vec![]),
        ]);
        let field = Arc::new(Field::new("nested", inner.data_type().clone(), true));
        let input: ArrayRef = Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(vec![0, 2, 3].into()),
            Arc::new(inner.clone()),
            None,
        ));
        let indexes: ArrayRef = Arc::new(Int64Array::from(vec![1, 1]));
        let result = lookup(&input, &indexes).unwrap();
        let expected = arrow::compute::take(&inner, &UInt32Array::from(vec![0, 2]), None).unwrap();
        assert_eq!(result.data_type(), inner.data_type());
        assert_eq!(result.as_ref(), expected.as_ref());
    }
}
