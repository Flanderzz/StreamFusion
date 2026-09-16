use arrow::array::{Array, ArrayRef, Int32Builder};
use arrow::datatypes::DataType;
use datafusion::common::{
    cast::{as_int32_array, as_string_array},
    exec_err, Result,
};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;

pub(super) fn function(arity: usize) -> ScalarUDF {
    let mut inputs = vec![DataType::Utf8, DataType::Utf8, DataType::Int32];
    if arity == 4 {
        inputs.push(DataType::Int32);
    }
    super::udf("flink_instr", inputs, DataType::Int32, instr)
}

fn instr(args: &[ArrayRef]) -> Result<ArrayRef> {
    if !(3..=4).contains(&args.len()) {
        return exec_err!("INSTR expects three or four arguments");
    }
    let strings = as_string_array(&args[0])?;
    let needles = as_string_array(&args[1])?;
    let starts = as_int32_array(&args[2])?;
    let occurrences = args.get(3).map(|arg| as_int32_array(arg)).transpose()?;
    let mut result = Int32Builder::with_capacity(strings.len());
    for row in 0..strings.len() {
        if strings.is_null(row)
            || needles.is_null(row)
            || starts.is_null(row)
            || occurrences.is_some_and(|values| values.is_null(row))
        {
            result.append_null();
            continue;
        }
        result.append_value(position(
            strings.value(row),
            needles.value(row),
            starts.value(row),
            occurrences.map_or(1, |values| values.value(row)),
        )?);
    }
    Ok(Arc::new(result.finish()))
}

fn position(source: &str, needle: &str, start: i32, occurrence: i32) -> Result<i32> {
    if occurrence <= 0 {
        return exec_err!("nthAppearance must be positive!");
    }
    if start == 0 {
        return Ok(0);
    }
    if start == i32::MIN {
        return exec_err!("INSTR startPosition overflows negation (Flink StackOverflowError)");
    }
    let ascii = source.is_ascii();
    if needle.is_empty() {
        return Ok(if start > 0 {
            1
        } else {
            (source.chars().count() as i32).wrapping_add(1)
        });
    }
    let skip = (start.unsigned_abs() - 1) as usize;
    let mut found = 0;
    if start > 0 {
        let mut from = if ascii {
            skip
        } else if let Some((offset, _)) = source.char_indices().nth(skip) {
            offset
        } else {
            return Ok(0);
        };
        let finder = memchr::memmem::Finder::new(needle.as_bytes());
        for _ in 0..occurrence {
            let Some(offset) = source
                .as_bytes()
                .get(from..)
                .and_then(|tail| finder.find(tail))
            else {
                return Ok(0);
            };
            found = from + offset;
            from = found
                + if ascii {
                    1
                } else {
                    source[found..].chars().next().unwrap().len_utf8()
                };
        }
    } else {
        let mut end = if ascii {
            source.len().saturating_sub(skip)
        } else if let Some((offset, ch)) = source.char_indices().rev().nth(skip) {
            offset + ch.len_utf8()
        } else {
            return Ok(0);
        };
        let finder = memchr::memmem::FinderRev::new(needle.as_bytes());
        let last_char_width = needle.chars().next_back().unwrap().len_utf8();
        for _ in 0..occurrence {
            let Some(offset) = finder.rfind(&source.as_bytes()[..end]) else {
                return Ok(0);
            };
            found = offset;
            // Flink advances one codepoint in the reversed string, allowing overlapping matches.
            end = found + needle.len() - last_char_width;
        }
    }
    Ok(if ascii {
        found
    } else {
        source[..found].chars().count()
    } as i32
        + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};

    #[test]
    fn forward_and_reverse_searches_count_overlapping_codepoints() {
        for (source, needle, start, occurrence, expected) in [
            ("abababa", "aba", 1, 3, 5),
            ("abababa", "aba", -1, 3, 1),
            ("abcabc", "bc", -2, 1, 2),
            ("aaaa", "aa", -1, 2, 2),
            (
                "\u{1f600}a\u{1f600}a\u{1f600}",
                "\u{1f600}a\u{1f600}",
                -1,
                2,
                1,
            ),
            ("e\u{301}e\u{301}e", "e\u{301}e", 2, 1, 3),
            ("a\0a\0a", "a\0a", -1, 2, 1),
        ] {
            assert_eq!(
                position(source, needle, start, occurrence).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn empty_needles_and_extreme_arguments_follow_flink() {
        for (source, needle, start, occurrence, expected) in [
            ("abc", "", i32::MAX, i32::MAX, 1),
            ("abc", "", -i32::MAX, 2, 4),
            ("", "", -1, 1, 1),
            ("abc", "", 0, 1, 0),
            ("abc", "a", -i32::MAX, 1, 0),
            ("abc", "a", i32::MAX, 1, 0),
            ("abc", "a", 1, i32::MAX, 0),
        ] {
            assert_eq!(
                position(source, needle, start, occurrence).unwrap(),
                expected
            );
        }
        assert!(position("", "", 0, 0).is_err());
        assert!(position("", "", i32::MIN, 1).is_err());
    }

    #[test]
    fn sliced_nulls_suppress_invalid_arguments() {
        let strings: ArrayRef =
            Arc::new(StringArray::from(vec![Some("ignored"), None, Some("aaaa")]));
        let needles: ArrayRef = Arc::new(StringArray::from(vec!["ignored", "a", "aa"]));
        let starts: ArrayRef = Arc::new(Int32Array::from(vec![1, i32::MIN, -1]));
        let occurrences: ArrayRef = Arc::new(Int32Array::from(vec![1, 0, 2]));
        let result = instr(&[
            strings.slice(1, 2),
            needles.slice(1, 2),
            starts.slice(1, 2),
            occurrences.slice(1, 2),
        ])
        .unwrap();
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(2)]));
        assert_eq!(&result, &expected);
    }
}
