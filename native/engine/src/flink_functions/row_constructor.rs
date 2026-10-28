//! Named Arrow fields with the copy boundary used by Flink's ROW and OBJECT constructors.
use std::sync::Arc;

use arrow::array::{Int8Array, StructArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

pub(super) fn function(binary: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(RowConstructor {
        binary,
        signature: Signature::variadic_any(Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct RowConstructor {
    binary: bool,
    signature: Signature,
}
impl ScalarUDFImpl for RowConstructor {
    fn name(&self) -> &str {
        if self.binary {
            "flink_binary_row_constructor"
        } else {
            "flink_object_constructor"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        exec_err!("ROW construction requires constant field names")
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let original = datafusion::functions::core::named_struct().return_field_from_args(args)?;
        let DataType::Struct(fields) = original.data_type() else {
            return exec_err!("ROW requires named fields");
        };
        let mut fields = fields.to_vec();
        let mut name = "$row_kind$".to_string();
        while fields.iter().any(|field| field.name() == &name) {
            name.push('$');
        }
        fields.push(Arc::new(
            Field::new(name, DataType::Int8, false).with_metadata(
                [("streamfusion.nested-row-kind".into(), "1".into())]
                    .into_iter()
                    .collect(),
            ),
        ));
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Struct(fields.into()),
            false,
        )))
    }
    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let DataType::Struct(fields) = args.return_type() else {
            return exec_err!("ROW requires struct output");
        };
        let fields = fields.clone();
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        args.return_field = Arc::new(Field::new(
            "row",
            DataType::Struct(fields.iter().take(fields.len() - 1).cloned().collect()),
            false,
        ));
        let output = datafusion::functions::core::named_struct()
            .invoke_with_args(args)?
            .to_array(rows)?;
        let original = datafusion::common::cast::as_struct_array(&output)?;
        let mut columns = original
            .columns()
            .iter()
            .map(|column| {
                if self.binary {
                    super::collection_string::cold(column)
                } else {
                    Ok(column.clone())
                }
            })
            .collect::<Result<Vec<_>>>()?;
        columns.push(Arc::new(Int8Array::from(vec![
            if self.binary {
                4
            } else {
                0
            };
            rows
        ])));
        let result =
            Arc::new(StructArray::try_new(fields, columns, None)?) as arrow::array::ArrayRef;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}
