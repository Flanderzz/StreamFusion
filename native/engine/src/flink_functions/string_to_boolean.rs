use arrow::array::{Array, ArrayRef, BooleanBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_string_array, exec_err, Result};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;

pub(super) fn function(null_on_error: bool) -> ScalarUDF {
    super::udf(
        if null_on_error {
            "flink_string_to_boolean_or_null"
        } else {
            "flink_string_to_boolean"
        },
        vec![DataType::Utf8],
        DataType::Boolean,
        if null_on_error {
            |args| cast(args, true)
        } else {
            |args| cast(args, false)
        },
    )
}

fn cast(args: &[ArrayRef], null_on_error: bool) -> Result<ArrayRef> {
    let [input] = args else {
        return exec_err!("STRING to BOOLEAN expects one argument");
    };
    let input = as_string_array(input)?;
    let mut result = BooleanBuilder::with_capacity(input.len());
    for value in input {
        let Some(value) = value else {
            result.append_null();
            continue;
        };
        let parsed = if ["t", "true", "y", "yes", "1"]
            .iter()
            .any(|token| value.eq_ignore_ascii_case(token))
        {
            Some(true)
        } else if ["f", "false", "n", "no", "0"]
            .iter()
            .any(|token| value.eq_ignore_ascii_case(token))
        {
            Some(false)
        } else {
            None
        };
        if parsed.is_none() && !null_on_error {
            return exec_err!("Cannot parse '{value}' as BOOLEAN.");
        }
        result.append_option(parsed);
    }
    Ok(Arc::new(result.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, StringArray};

    #[test]
    fn accepted_tokens_preserve_nulls_and_sliced_offsets() {
        let values = [
            Some("unused"),
            Some("t"),
            Some("TRUE"),
            Some("y"),
            Some("yEs"),
            Some("1"),
            None,
            Some("f"),
            Some("FaLsE"),
            Some("n"),
            Some("NO"),
            Some("0"),
        ];
        let input: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
        let expected: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
        ]));
        for null_on_error in [false, true] {
            assert_eq!(
                &cast(&[input.slice(1, 11)], null_on_error).unwrap(),
                &expected
            );
        }
    }

    #[test]
    fn malformed_tokens_are_errors_or_nulls_without_trimming() {
        for value in [
            "",
            " true",
            "false ",
            "\ttrue",
            "false\n",
            "2",
            "-1",
            "on",
            "off",
            "\u{662f}",
            "\u{ff34}rue",
            "tr\0ue",
            "true\u{a0}",
        ] {
            let input: ArrayRef = Arc::new(StringArray::from(vec![value]));
            let error = cast(&[input.clone()], false).unwrap_err();
            assert!(error
                .to_string()
                .contains(&format!("Cannot parse '{value}' as BOOLEAN.")));
            let result = cast(&[input], true).unwrap();
            assert_eq!(result.data_type(), &DataType::Boolean);
            assert_eq!(result.null_count(), 1);
        }
    }

    #[test]
    fn empty_and_all_null_inputs_are_boolean_arrays() {
        for len in [0, 3] {
            let input: ArrayRef = Arc::new(StringArray::new_null(len));
            let result = cast(&[input], false).unwrap();
            assert_eq!(result.data_type(), &DataType::Boolean);
            assert_eq!(result.len(), len);
            assert_eq!(result.null_count(), len);
        }
    }
}
