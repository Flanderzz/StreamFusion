use super::json_path::{Path, Reader, Value};
use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_string_array, exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(JsonValue {
        signature: Signature::exact(vec![DataType::Utf8; 7], Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonValue {
    signature: Signature,
}

impl ScalarUDFImpl for JsonValue {
    fn name(&self) -> &str {
        "flink_json_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input, path, empty, empty_default, error, error_default, unicode] =
            args.args.as_slice()
        else {
            return exec_err!("JSON_VALUE expects input, path and EMPTY/ERROR policies");
        };
        let path_text = literal(path)?.ok_or_else(|| {
            datafusion::common::exec_datafusion_err!("JSON_VALUE requires a path")
        })?;
        let path = Path::parse(path_text, literal(unicode)?.unwrap_or("")).ok_or_else(|| {
            datafusion::common::exec_datafusion_err!("Unsupported JSON_VALUE path")
        })?;
        let empty = Behavior::new(empty, empty_default)?;
        let error = Behavior::new(error, error_default)?;
        let scalar = matches!(input, ColumnarValue::Scalar(_));
        let input = input.to_array(if scalar { 1 } else { args.number_rows })?;
        let input = as_string_array(&input)?;
        let mut output = StringBuilder::with_capacity(input.len(), input.len() * 8);
        let mut reader = Reader::new();
        for document in input.iter() {
            let Some(document) = document else {
                output.append_null();
                continue;
            };
            match reader.read(&path, document) {
                Ok(Value::Missing | Value::Null) => output.append_option(empty.apply("EMPTY")?),
                Ok(Value::Container) if path.lax => output.append_option(empty.apply("EMPTY")?),
                Ok(Value::Container) | Err(()) => output.append_option(error.apply("ERROR")?),
                Ok(value) => output.append_option(value.text()),
            }
        }
        finish(Arc::new(output.finish()), scalar)
    }
}

enum Behavior<'a> {
    Null,
    Error,
    Default(Option<&'a str>),
}

impl<'a> Behavior<'a> {
    fn new(behavior: &'a ColumnarValue, default: &'a ColumnarValue) -> Result<Self> {
        match literal(behavior)? {
            Some("NULL") => Ok(Self::Null),
            Some("ERROR") => Ok(Self::Error),
            Some("DEFAULT") => Ok(Self::Default(literal(default)?)),
            _ => exec_err!("Unsupported JSON_VALUE policy"),
        }
    }

    fn apply(&self, mode: &str) -> Result<Option<&str>> {
        match self {
            Self::Null => Ok(None),
            Self::Error => exec_err!("JSON_VALUE {mode} result is not allowed"),
            Self::Default(value) => Ok(*value),
        }
    }
}

pub(super) fn literal(value: &ColumnarValue) -> Result<Option<&str>> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => Ok(value.as_deref()),
        _ => exec_err!("SQL/JSON path and policies must be string literals"),
    }
}

pub(super) fn finish(output: ArrayRef, scalar: bool) -> Result<ColumnarValue> {
    if scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &output, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::Field;

    fn invoke(input: ColumnarValue, empty: &str, error: &str) -> Result<ColumnarValue> {
        let rows = match &input {
            ColumnarValue::Array(array) => array.len(),
            _ => 1,
        };
        let mut args = vec![input];
        args.extend(
            ["lax $.a", empty, "empty", error, "error", "13.0"]
                .map(|s| ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.into())))),
        );
        function().invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
            config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
        })
    }

    #[test]
    fn sliced_nullable_inputs_and_empty_batches() {
        let input = StringArray::from(vec![
            Some("unused"),
            Some(r#"{"a":"ok"}"#),
            None,
            Some("{}"),
            Some("null"),
            Some("tail"),
        ]);
        let input = input.slice(1, 4);
        let ColumnarValue::Array(output) =
            invoke(ColumnarValue::Array(Arc::new(input)), "DEFAULT", "DEFAULT").unwrap()
        else {
            panic!("expected array")
        };
        output.to_data().validate_full().unwrap();
        assert_eq!(
            as_string_array(&output).unwrap(),
            &StringArray::from(vec![Some("ok"), None, Some("empty"), Some("error")])
        );
        let empty = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
        let ColumnarValue::Array(output) =
            invoke(ColumnarValue::Array(empty), "ERROR", "ERROR").unwrap()
        else {
            panic!("expected array")
        };
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn scalar_null_errors_and_arity() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        assert!(matches!(
            invoke(input, "ERROR", "ERROR").unwrap(),
            ColumnarValue::Scalar(ScalarValue::Utf8(None))
        ));
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("{}".into())));
        assert!(invoke(input, "ERROR", "DEFAULT")
            .unwrap_err()
            .to_string()
            .contains("JSON_VALUE EMPTY"));
        assert!(function().coerce_types(&[]).is_err());
    }
}
