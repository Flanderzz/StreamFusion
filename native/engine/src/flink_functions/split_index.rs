use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{
    cast::{as_int32_array, as_string_array},
    exec_err, Result,
};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;
use streamfusion_bridge::string_cache::JavaStringCacheColumn;

/// The released integer overload treats 1..=255 as a character, not a decimal string.
pub(crate) fn integer_separator(args: [&ArrayRef; 3], caches: &[ArrayRef]) -> Option<ArrayRef> {
    let [input, separator, index] = args;
    let input = as_string_array(input).ok()?;
    let separator = as_int32_array(separator).ok()?;
    let index = as_int32_array(index).ok()?;
    let cache = caches
        .first()
        .filter(|cache| cache.data_type() != &DataType::Null)
        .map(JavaStringCacheColumn::new);
    let mut output = StringBuilder::with_capacity(input.len(), input.values().len());
    for row in 0..input.len() {
        if input.is_null(row)
            || separator.is_null(row)
            || index.is_null(row)
            || !(1..=255).contains(&separator.value(row))
            || index.value(row) < 0
        {
            output.append_null();
            continue;
        }
        let value = input.value(row);
        if cache
            .as_ref()
            .and_then(|cache| cache.value(row))
            .is_some_and(|units| {
                if value.is_ascii() {
                    units.len() != value.len()
                        || units
                            .iter()
                            .zip(value.as_bytes())
                            .fold(0u16, |difference, (&unit, &byte)| {
                                difference | (unit ^ u16::from(byte))
                            })
                            != 0
                } else {
                    !units.iter().copied().eq(value.encode_utf16())
                }
            })
        {
            return None;
        }
        if value.is_empty() {
            output.append_null();
            continue;
        }
        let delimiter = separator.value(row) as u8;
        let index = index.value(row) as usize;
        let token = if delimiter.is_ascii() {
            let mut positions = memchr::memchr_iter(delimiter, value.as_bytes());
            let start = if index == 0 {
                Some(0)
            } else {
                positions.nth(index - 1).map(|position| position + 1)
            };
            start.map(|start| &value[start..positions.next().unwrap_or(value.len())])
        } else {
            value.split(char::from(delimiter)).nth(index)
        };
        output.append_option(token);
    }
    Some(Arc::new(output.finish()))
}

pub(crate) fn function() -> ScalarUDF {
    super::udf(
        "flink_split_index",
        vec![DataType::Utf8, DataType::Utf8, DataType::Int32],
        DataType::Utf8,
        split_index,
    )
}

fn split_index(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input, separator, index] = args else {
        return exec_err!("SPLIT_INDEX expects three arguments");
    };
    let input = as_string_array(input)?;
    let separator = as_string_array(separator)?;
    let index = as_int32_array(index)?;
    let mut output = StringBuilder::with_capacity(input.len(), input.values().len());
    for row in 0..input.len() {
        if input.is_null(row)
            || separator.is_null(row)
            || index.is_null(row)
            || index.value(row) < 0
            || input.value(row).is_empty()
        {
            output.append_null();
            continue;
        }
        let token = if separator.value(row).is_empty() {
            input
                .value(row)
                .split(java_whitespace)
                .nth(index.value(row) as usize)
        } else {
            input
                .value(row)
                .split(separator.value(row))
                .nth(index.value(row) as usize)
        };
        match token {
            Some(value) => output.append_value(value),
            None => output.append_null(),
        }
    }
    Ok(Arc::new(output.finish()))
}

fn java_whitespace(ch: char) -> bool {
    matches!(ch, '\u{0009}'..='\u{000d}' | '\u{001c}'..='\u{0020}' | '\u{1680}'
        | '\u{2000}'..='\u{2006}' | '\u{2008}'..='\u{200a}' | '\u{2028}' | '\u{2029}'
        | '\u{205f}' | '\u{3000}')
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use streamfusion_bridge::string_cache::string_cache_array;

    #[test]
    fn integer_delimiters_preserve_empty_tokens_latin1_nulls_and_slices() {
        let input = Arc::new(StringArray::from(vec![
            Some("skip"),
            Some(",a,,"),
            Some(",a,,"),
            Some("a\u{ff}b\u{ff}"),
            Some("\u{1f600},\u{4e2d}"),
            Some(""),
            Some("x"),
            Some("x"),
            Some("x"),
            Some("x"),
            Some("x"),
            None,
            Some("x"),
            Some("x"),
        ])) as ArrayRef;
        let delimiters = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(44),
            Some(44),
            Some(255),
            Some(44),
            Some(44),
            Some(0),
            Some(256),
            Some(i32::MIN),
            Some(i32::MAX),
            Some(44),
            Some(44),
            None,
            Some(44),
        ])) as ArrayRef;
        let indices = Arc::new(Int32Array::from(vec![
            Some(0),
            Some(0),
            Some(3),
            Some(1),
            Some(1),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(-1),
            Some(0),
            Some(0),
            None,
        ])) as ArrayRef;
        let args = [
            input.slice(1, 13),
            delimiters.slice(1, 13),
            indices.slice(1, 13),
        ];
        let expected = StringArray::from(vec![
            Some(""),
            Some(""),
            Some("b"),
            Some("\u{4e2d}"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ]);
        assert_eq!(
            integer_separator(args.each_ref(), &[]).unwrap().as_ref(),
            &expected
        );
        let empty = args.map(|arg| arg.slice(0, 0));
        assert_eq!(integer_separator(empty.each_ref(), &[]).unwrap().len(), 0);
    }

    #[test]
    fn original_utf16_must_match_before_searching_unicode() {
        let args = [
            Arc::new(StringArray::from(vec!["\u{1f600},?"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![44])),
            Arc::new(Int32Array::from(vec![1])),
        ];
        let valid = [0xd83d, 0xde00, 44, 63];
        let invalid = [0xd83d, 0xde00, 44, 0xd800];
        assert_eq!(
            integer_separator(
                args.each_ref(),
                &[string_cache_array([Some(valid.as_slice())])]
            )
            .unwrap()
            .as_ref(),
            &StringArray::from(vec!["?"])
        );
        assert!(integer_separator(
            args.each_ref(),
            &[string_cache_array([Some(invalid.as_slice())])]
        )
        .is_none());
    }
}
