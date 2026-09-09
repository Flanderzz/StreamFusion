use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder};
use datafusion::common::{cast::as_string_array, Result};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;

use super::charset::Charset;

pub(crate) fn function() -> ScalarUDF {
    super::charset::function(false)
}

pub(super) fn encode(input: &ArrayRef, charset: Charset) -> Result<ArrayRef> {
    let input = as_string_array(input)?;
    if matches!(charset, Charset::Utf8) {
        return Ok(Arc::new(BinaryArray::new(
            input.offsets().clone(),
            input.values().clone(),
            input.nulls().cloned(),
        )));
    }
    let limit = if matches!(charset, Charset::Ascii) {
        127
    } else {
        255
    };
    let mut output = BinaryBuilder::with_capacity(input.len(), input.values().len());
    let mut bytes = Vec::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        bytes.clear();
        bytes.extend(
            input
                .value(row)
                .chars()
                .map(|c| if c as u32 <= limit { c as u8 } else { b'?' }),
        );
        output.append_value(&bytes);
    }
    Ok(Arc::new(output.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn encode_keeps_slices_nulls_and_jdk_replacement() {
        let input = StringArray::from(vec![
            Some("unused"),
            Some("a\u{e9}\u{1f600}"),
            None,
            Some(""),
        ])
        .slice(1, 3);
        for (charset, expected) in [
            (Charset::Utf8, "a\u{e9}\u{1f600}".as_bytes()),
            (Charset::Latin1, b"a\xe9?"),
            (Charset::Ascii, b"a??"),
        ] {
            let input: ArrayRef = Arc::new(input.clone());
            let output = encode(&input, charset).unwrap();
            let output = output.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(output.value(0), expected);
            assert!(output.is_null(1));
            assert_eq!(output.value(2), b"");
        }
    }
}
