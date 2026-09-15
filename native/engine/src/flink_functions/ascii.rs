use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_string_array, exec_err, Result};
use datafusion::logical_expr::ScalarUDF;

pub(super) fn function() -> ScalarUDF {
    super::udf("flink_ascii", vec![DataType::Utf8], DataType::Int32, ascii)
}

fn ascii(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = args else {
        return exec_err!("ASCII expects one character argument");
    };
    let input = as_string_array(input)?;
    // Flink's generated call widens a Java byte, not a Unicode code point.
    let values = (0..input.len())
        .map(|row| {
            input
                .value(row)
                .as_bytes()
                .first()
                .map_or(0, |b| *b as i8 as i32)
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Int32Array::new(
        values.into(),
        input.nulls().cloned(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use datafusion::common::cast::as_int32_array;

    #[test]
    fn signed_first_byte_empty_null_and_sliced_rows() {
        let input = StringArray::from(vec![
            Some("skip"),
            Some(""),
            Some("\0"),
            Some("A"),
            Some("\u{e9}"),
            Some("\u{4e2d}"),
            Some("\u{1f600}"),
            None,
        ])
        .slice(1, 7);
        let result = ascii(&[Arc::new(input)]).unwrap();
        assert_eq!(
            as_int32_array(&result).unwrap(),
            &Int32Array::from(vec![
                Some(0),
                Some(0),
                Some(65),
                Some(-61),
                Some(-28),
                Some(-16),
                None,
            ])
        );
        result.to_data().validate_full().unwrap();
        assert!(ascii(&[]).is_err());
    }
}
