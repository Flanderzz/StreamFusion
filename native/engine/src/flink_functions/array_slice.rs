#[cfg(test)]
use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayRef, Int32Array, ListArray, MutableArrayData};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_list_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function(arity: usize) -> ScalarUDF {
    ScalarUDF::new_from_impl(ArraySlice {
        signature: Signature::any(arity, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArraySlice {
    signature: Signature,
}

impl ScalarUDFImpl for ArraySlice {
    fn name(&self) -> &str {
        "flink_array_slice"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [list @ DataType::List(_), DataType::Int32]
            | [list @ DataType::List(_), DataType::Int32, DataType::Int32] => Ok(list.clone()),
            _ => exec_err!("ARRAY_SLICE requires an ARRAY and one or two INT bounds"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(slice, vec![])(&args.args)
    }
}

fn slice(args: &[ArrayRef]) -> Result<ArrayRef> {
    let array = as_list_array(&args[0])?;
    let start = args[1].as_any().downcast_ref::<Int32Array>().unwrap();
    let end = args
        .get(2)
        .map(|value| value.as_any().downcast_ref::<Int32Array>().unwrap());
    let DataType::List(field) = array.data_type() else {
        unreachable!()
    };
    let mut offsets = Vec::with_capacity(array.len() + 1);
    let mut ranges = Vec::with_capacity(array.len());
    let mut copied = Vec::with_capacity(array.len());
    let mut valid = Vec::with_capacity(array.len());
    offsets.push(0);
    let mut unchanged = true;
    for row in 0..array.len() {
        let parent_valid = !array.is_null(row)
            && !start.is_null(row)
            && end.is_none_or(|values| !values.is_null(row));
        valid.push(parent_valid);
        let first = array.value_offsets()[row];
        let length = array.value_offsets()[row + 1] - first;
        let mut range = first..first;
        if parent_valid && length > 0 {
            let normalize = |index: i32| {
                if index < 0 {
                    index.wrapping_add(length).wrapping_add(1)
                } else {
                    index
                }
            };
            let from = normalize(start.value(row)).max(1);
            let last = normalize(end.map_or(length, |values| values.value(row)));
            let last = if last == 0 { 1 } else { last.min(length) };
            unchanged &= from == 1 && last == length;
            if from <= last {
                range = first + from - 1..first + last;
            }
        }
        unchanged &= parent_valid != array.is_null(row);
        offsets.push(offsets.last().unwrap() + (range.end - range.start));
        copied.push(range.start != first || range.end != first + length);
        ranges.push(range);
    }
    if unchanged {
        return Ok(args[0].clone());
    }
    let original = array.values().to_data();
    let mut values =
        MutableArrayData::new(vec![&original], false, *offsets.last().unwrap() as usize);
    for range in ranges {
        values.extend(0, range.start as usize, range.end as usize);
    }
    let result = ListArray::new(
        field.clone(),
        OffsetBuffer::new(offsets.into()),
        make_array(values.freeze()),
        Some(NullBuffer::from(valid)),
    );
    super::array_copy::generic_elements(&result, |row| copied[row])
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Builder, ListBuilder};

    #[test]
    fn slices_keep_inclusive_java_bounds_nulls_and_offsets() {
        let mut lists = ListBuilder::new(Int32Builder::new());
        for row in 0..8 {
            for value in [Some(1), None, Some(3), Some(4)] {
                lists.values().append_option(value);
            }
            lists.append(row != 6);
        }
        let input: ArrayRef = Arc::new(lists.finish().slice(1, 7));
        let starts: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(0),
            Some(-2),
            Some(i32::MIN),
            Some(5),
            None,
            Some(1),
            Some(1),
        ]));
        let ends: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(0),
            Some(-1),
            Some(i32::MAX),
            Some(1),
            Some(4),
            Some(4),
            None,
        ]));
        let output = slice(&[input, starts, ends]).unwrap();
        let expected = ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>([
            Some(vec![Some(1)]),
            Some(vec![Some(3), Some(4)]),
            Some(vec![Some(1), None, Some(3), Some(4)]),
            Some(vec![]),
            None,
            None,
            None,
        ]);
        assert_eq!(output.as_ref(), &expected);
    }

    #[test]
    fn a_whole_slice_reuses_the_original_arrays() {
        let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<
            arrow::datatypes::Int32Type,
            _,
            _,
        >([Some(vec![Some(1), None]), Some(vec![]), None]));
        let starts: ArrayRef = Arc::new(Int32Array::from(vec![1, i32::MAX, 0]));
        assert!(Arc::ptr_eq(&slice(&[list.clone(), starts]).unwrap(), &list));
    }
}
