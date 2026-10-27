use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBufferBuilder, ListArray, NullBufferBuilder,
    PrimitiveArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal128Type, Int16Type, Int32Type, Int64Type, Int8Type,
};
use datafusion::common::{cast::as_list_array, exec_err, Result};
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
        "flink_array_sort"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if !(2..=3).contains(&types.len()) || types[1..].iter().any(|t| t != &DataType::Boolean) {
            return exec_err!("ARRAY_SORT requires an ARRAY and one or two Boolean options");
        }
        match &types[0] {
            DataType::List(field)
                if field.is_nullable()
                    && matches!(
                        field.data_type(),
                        DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64
                            | DataType::Decimal128(_, _)
                    ) =>
            {
                Ok(types[0].clone())
            }
            _ => exec_err!("ARRAY_SORT requires nullable integer or DECIMAL elements"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(sort, vec![])(&args.args)
    }
}

fn sort(args: &[ArrayRef]) -> Result<ArrayRef> {
    let array = as_list_array(&args[0])?;
    let ascending = args[1].as_any().downcast_ref::<BooleanArray>().unwrap();
    let null_first = args.get(2).map_or(ascending, |a| {
        a.as_any().downcast_ref::<BooleanArray>().unwrap()
    });
    // The planner proves fewer than 32 elements, or distinct values. Flink's inconsistent
    // equality comparator can otherwise make Java TimSort fail on long duplicate arrays.
    match array.value_type() {
        DataType::Int8 => sort_typed::<Int8Type>(array, ascending, null_first),
        DataType::Int16 => sort_typed::<Int16Type>(array, ascending, null_first),
        DataType::Int32 => sort_typed::<Int32Type>(array, ascending, null_first),
        DataType::Int64 => sort_typed::<Int64Type>(array, ascending, null_first),
        DataType::Decimal128(_, _) => sort_typed::<Decimal128Type>(array, ascending, null_first),
        _ => exec_err!("ARRAY_SORT has an unsupported element type"),
    }
}

fn sort_typed<T: ArrowPrimitiveType>(
    array: &ListArray,
    ascending: &BooleanArray,
    null_first: &BooleanArray,
) -> Result<ArrayRef>
where
    T::Native: Ord,
{
    let source = array
        .values()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap();
    let offsets = array.value_offsets();
    let base = offsets[0] as usize;
    let total = offsets[array.len()] as usize - base;
    let mut parent_nulls = NullBufferBuilder::new(array.len());
    let mut values = if source.null_count() == 0 {
        source.values()[base..base + total].to_vec()
    } else {
        vec![T::Native::default(); total]
    };
    let mut validity = BooleanBufferBuilder::new(total);
    for (row, bounds) in offsets.windows(2).enumerate() {
        let start = bounds[0] as usize;
        let end = bounds[1] as usize;
        let valid = array.is_valid(row) && ascending.is_valid(row) && null_first.is_valid(row);
        parent_nulls.append(valid);
        if !valid {
            if source.null_count() != 0 {
                validity.append_n(end - start, false);
            }
            continue;
        }
        let row_start = start - base;
        let non_null = if source.null_count() == 0 {
            row_start..end - base
        } else {
            let nulls = source
                .nulls()
                .unwrap()
                .slice(start, end - start)
                .null_count();
            let first = if null_first.value(row) { nulls } else { 0 };
            let mut next = row_start + first;
            for index in start..end {
                if source.is_valid(index) {
                    values[next] = source.value(index);
                    next += 1;
                }
            }
            validity.append_n(first, false);
            validity.append_n(end - start - nulls, true);
            validity.append_n(nulls - first, false);
            row_start + first..next
        };
        if ascending.value(row) {
            values[non_null].sort_unstable();
        } else {
            values[non_null].sort_unstable_by(|a, b| b.cmp(a));
        }
    }
    let element_nulls = (source.null_count() != 0).then(|| NullBuffer::new(validity.finish()));
    let values = PrimitiveArray::<T>::new(values.into(), element_nulls)
        .with_data_type(source.data_type().clone());
    let DataType::List(field) = array.data_type() else {
        unreachable!()
    };
    Ok(Arc::new(ListArray::new(
        field.clone(),
        OffsetBuffer::new(
            offsets
                .iter()
                .map(|offset| offset - offsets[0])
                .collect::<Vec<_>>()
                .into(),
        ),
        Arc::new(values),
        parent_nulls.finish(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ListBuilder, PrimitiveBuilder};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn options_nulls_duplicates_and_slices_keep_their_own_rows() {
        let list = Arc::new(
            ListArray::from_iter_primitive::<Int64Type, _, _>([
                Some(vec![Some(99)]),
                Some(vec![Some(i64::MAX), None, Some(i64::MIN), Some(i64::MAX)]),
                Some(vec![Some(3), Some(1), None]),
                Some(vec![Some(3), Some(1), None]),
                Some(vec![Some(3), Some(1), None]),
                Some(vec![]),
                None,
                Some(vec![Some(1)]),
                Some(vec![Some(1)]),
            ])
            .slice(1, 8),
        ) as ArrayRef;
        let ascending = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            None,
            Some(true),
        ])) as ArrayRef;
        let null_first = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            None,
        ])) as ArrayRef;
        let actual = sort(&[list, ascending, null_first]).unwrap();
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![None, Some(i64::MIN), Some(i64::MAX), Some(i64::MAX)]),
            Some(vec![Some(1), Some(3), None]),
            Some(vec![None, Some(3), Some(1)]),
            Some(vec![Some(3), Some(1), None]),
            Some(vec![]),
            None,
            None,
            None,
        ]);
        assert_eq!(actual.as_ref(), &expected);
        assert_eq!(
            sort(&[
                actual.slice(0, 0),
                Arc::new(BooleanArray::from(Vec::<bool>::new()))
            ])
            .unwrap()
            .len(),
            0
        );
    }

    #[test]
    fn decimal_precision_and_two_argument_defaults_are_preserved() {
        let element =
            PrimitiveBuilder::<Decimal128Type>::new().with_data_type(DataType::Decimal128(38, 9));
        let mut lists = ListBuilder::new(element);
        for _ in 0..2 {
            lists
                .values()
                .append_value(90_071_992_547_409_930_000_000_001);
            lists.values().append_null();
            lists
                .values()
                .append_value(-90_071_992_547_409_930_000_000_001);
            lists.append(true);
        }
        let input = Arc::new(lists.finish()) as ArrayRef;
        let flags = Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef;
        let output = sort(&[input.clone(), flags]).unwrap();
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(output.data_type(), input.data_type());
        let first = output.value(0);
        let first = first
            .as_any()
            .downcast_ref::<PrimitiveArray<Decimal128Type>>()
            .unwrap();
        assert!(first.is_null(0));
        assert_eq!(first.value(1), -90_071_992_547_409_930_000_000_001);
        assert_eq!(first.value(2), 90_071_992_547_409_930_000_000_001);
        let second = output.value(1);
        let second = second
            .as_any()
            .downcast_ref::<PrimitiveArray<Decimal128Type>>()
            .unwrap();
        assert_eq!(second.value(0), first.value(2));
        assert_eq!(second.value(1), first.value(1));
        assert!(second.is_null(2));
    }

    #[test]
    fn compiled_runtime_options_execute_without_a_jvm() {
        use crate::calc::CalcExpression;
        let list = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(2), Some(1)]),
            Some(vec![]),
            None,
        ])) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", list.data_type().clone(), true),
                Field::new("asc", DataType::Boolean, true),
            ])),
            vec![
                list,
                Arc::new(BooleanArray::from(vec![Some(false), None, Some(true)])),
            ],
        )
        .unwrap();
        let mut calc = CalcExpression {
            kinds: vec![6, 0, 0],
            payload: vec![217, 0, 1],
            child_counts: vec![2, 0, 0],
            longs: vec![],
            doubles: vec![],
            strings: vec![],
            projection_roots: vec![0],
            condition_root: -1,
            output_names: vec!["v".into()],
            compiled: None,
        };
        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(2), Some(1)]),
            None,
            None,
        ]);
        for _ in 0..2 {
            assert_eq!(calc.evaluate(batch.clone()).column(0).as_ref(), &expected);
        }
        assert_eq!(calc.evaluate(batch.slice(0, 0)).num_rows(), 0);
    }
}
