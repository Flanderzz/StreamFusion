use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_int64_array, exec_err, Result};
use datafusion::logical_expr::ScalarUDF;

pub(super) fn function() -> ScalarUDF {
    super::udf("flink_chr", vec![DataType::Int64], DataType::Utf8, chr)
}

fn chr(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = args else {
        return exec_err!("CHR expects one integer argument");
    };
    let input = as_int64_array(input)?;
    let mut output = StringBuilder::with_capacity(input.len(), input.len() * 2);
    let mut utf8 = [0u8; 4];
    for value in input {
        match value {
            None => output.append_null(),
            Some(value) if value < 0 => output.append_value(""),
            Some(value) => output.append_value(char::from(value as u8).encode_utf8(&mut utf8)),
        }
    }
    Ok(Arc::new(output.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use datafusion::common::cast::as_string_array;

    #[test]
    fn low_byte_null_negative_and_long_boundaries() {
        let input = Int64Array::from(vec![
            Some(99),
            None,
            Some(i64::MIN),
            Some(-1),
            Some(0),
            Some(128),
            Some(255),
            Some(256),
            Some(353),
            Some(i64::MAX),
        ])
        .slice(1, 9);
        let output = chr(&[Arc::new(input)]).unwrap();
        assert_eq!(
            as_string_array(&output).unwrap(),
            &StringArray::from(vec![
                None,
                Some(""),
                Some(""),
                Some("\0"),
                Some("\u{80}"),
                Some("\u{ff}"),
                Some("\0"),
                Some("a"),
                Some("\u{ff}"),
            ])
        );
        output.to_data().validate_full().unwrap();
        assert!(chr(&[]).is_err());
    }
}
