use arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use datafusion::common::{
    cast::{as_binary_array, as_string_array},
    Result,
};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;

use super::charset::Charset;

pub(crate) fn function() -> ScalarUDF {
    super::charset::function(true)
}

pub(super) fn decode(input: &ArrayRef, charset: Charset) -> Result<ArrayRef> {
    let input = as_binary_array(input)?;
    if matches!(charset, Charset::Utf8) {
        if let Ok(strings) = StringArray::try_new(
            input.offsets().clone(),
            input.values().clone(),
            input.nulls().cloned(),
        ) {
            return Ok(Arc::new(strings));
        }
    }
    let mut output = StringBuilder::with_capacity(input.len(), input.values().len());
    let mut text = String::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        text.clear();
        match charset {
            Charset::Utf16 | Charset::Utf16Be | Charset::Utf16Le => {
                return datafusion::common::exec_err!("Unverified DECODE charset");
            }
            Charset::Utf8 => super::scalar::append_java_utf8(input.value(row), &mut text),
            Charset::Latin1 => text.extend(input.value(row).iter().map(|&byte| char::from(byte))),
            Charset::Ascii => text.extend(input.value(row).iter().map(|&byte| {
                if byte < 128 {
                    char::from(byte)
                } else {
                    '\u{fffd}'
                }
            })),
        }
        super::check_string_capacity(output.values_slice().len(), text.len())?;
        output.append_value(&text);
    }
    Ok(Arc::new(output.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, StringArray};

    #[test]
    fn decode_keeps_jdk_malformed_sequence_grouping() {
        let input: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(&b"\xed\xa0\x80"[..]),
            Some(&b"\xf0\x90\x80"[..]),
            None,
        ]));
        let output = decode(&input, Charset::Utf8).unwrap();
        assert_eq!(
            as_string_array(&output).unwrap(),
            &StringArray::from(vec![Some("\u{fffd}"), Some("\u{fffd}"), None])
        );
    }
    #[test]
    fn valid_utf8_reuses_values_offsets_and_validity_on_a_slice() {
        let input = BinaryArray::from(vec![
            Some(&b"skip"[..]),
            Some("a\u{1f600}\0".as_bytes()),
            None,
            Some(&b""[..]),
        ])
        .slice(1, 3);
        let values = input.values().as_ptr();
        let offsets = input.offsets().as_ptr();
        let input: ArrayRef = Arc::new(input);
        let output = decode(&input, Charset::Utf8).unwrap();
        let strings = as_string_array(&output).unwrap();
        assert_eq!(
            strings,
            &StringArray::from(vec![Some("a\u{1f600}\0"), None, Some("")])
        );
        assert_eq!(strings.values().as_ptr(), values);
        assert_eq!(strings.offsets().as_ptr(), offsets);
        strings.to_data().validate_full().unwrap();
    }
}
