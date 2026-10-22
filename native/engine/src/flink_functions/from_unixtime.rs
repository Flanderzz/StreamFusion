//! Constant numeric SimpleDateFormat patterns with JDK-provided finite offset histories.

use arrow::array::{ArrayRef, Int64Array, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::fmt::Write;
use std::sync::Arc;

pub(crate) fn function(offset_millis: i64, pattern: &str) -> Result<ScalarUDF> {
    with_offsets(offset_millis, vec![], pattern)
}

pub(crate) fn from_encoded(encoded: &str, pattern: &str) -> Result<ScalarUDF> {
    let mut parts = encoded.split(',');
    let invalid = || datafusion::common::DataFusionError::Plan("invalid time-zone offsets".into());
    let initial = parts
        .next()
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let transitions = parts
        .map(|part| {
            let (millis, offset) = part.split_once(':').ok_or_else(invalid)?;
            Ok((
                millis.parse().map_err(|_| invalid())?,
                offset.parse().map_err(|_| invalid())?,
            ))
        })
        .collect::<Result<Vec<(i64, i64)>>>()?;
    if transitions.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(invalid());
    }
    with_offsets(initial, transitions, pattern)
}

fn with_offsets(
    offset_millis: i64,
    transitions: Vec<(i64, i64)>,
    pattern: &str,
) -> Result<ScalarUDF> {
    Ok(ScalarUDF::new_from_impl(FromUnixTime {
        offset_millis,
        transitions,
        tokens: compile(pattern)?,
        signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum Token {
    Literal(String),
    Field(char, usize),
}

fn compile(pattern: &str) -> Result<Vec<Token>> {
    let mut chars = pattern.chars().peekable();
    let mut quoted = false;
    let mut literal = String::new();
    let mut tokens = Vec::new();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            if chars.peek() == Some(&'\'') {
                chars.next();
                literal.push('\'');
            } else {
                quoted = !quoted;
            }
        } else if !quoted && ch.is_ascii_alphabetic() {
            if !literal.is_empty() {
                tokens.push(Token::Literal(std::mem::take(&mut literal)));
            }
            let mut width = 1;
            while chars.peek() == Some(&ch) {
                chars.next();
                width += 1;
            }
            if !(ch == 'y' && width == 4 || matches!(ch, 'M' | 'd' | 'H' | 'm' | 's') && width == 2)
            {
                return exec_err!("unsupported native FROM_UNIXTIME pattern");
            }
            tokens.push(Token::Field(ch, width));
        } else {
            literal.push(ch);
        }
    }
    if quoted {
        return exec_err!("unterminated FROM_UNIXTIME literal");
    }
    if !literal.is_empty() {
        tokens.push(Token::Literal(literal));
    }
    Ok(tokens)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FromUnixTime {
    offset_millis: i64,
    transitions: Vec<(i64, i64)>,
    tokens: Vec<Token>,
    signature: Signature,
}

impl FromUnixTime {
    #[cfg(test)]
    fn format(&self, seconds: i64) -> String {
        let mut output = String::with_capacity(32);
        self.format_into(seconds, &mut output);
        output
    }

    fn format_into(&self, seconds: i64, output: &mut String) {
        // Flink multiplies seconds with Java long overflow. GregorianCalendar then adds the
        // zone offset without overflowing the millisecond range at either endpoint.
        let millis = seconds.wrapping_mul(1000);
        let index = self
            .transitions
            .partition_point(|(start, _)| *start <= millis);
        let offset = if index == 0 {
            self.offset_millis
        } else {
            self.transitions[index - 1].1
        };
        let local = i128::from(millis) + i128::from(offset);
        let days = local.div_euclid(86_400_000) as i64;
        let time = local.rem_euclid(86_400_000) as i64;
        let (year, month, day) = calendar_date(days);
        for token in &self.tokens {
            match token {
                Token::Literal(text) => output.push_str(text),
                Token::Field(field, width) => {
                    let value = match field {
                        'y' => {
                            if year <= 0 {
                                1 - year
                            } else {
                                year
                            }
                        }
                        'M' => month,
                        'd' => day,
                        'H' => time / 3_600_000,
                        'm' => time / 60_000 % 60,
                        's' => time / 1000 % 60,
                        _ => unreachable!("compiled numeric field"),
                    };
                    write!(output, "{value:0width$}").unwrap();
                }
            }
        }
    }
}

fn calendar_date(days: i64) -> (i64, i64, i64) {
    let julian_day = days + 2_440_588;
    // SimpleDateFormat's default GregorianCalendar switches after 1582-10-04 (Julian)
    // to 1582-10-15 (Gregorian). The cutoff is a local calendar day.
    let (century, remaining) = if julian_day >= 2_299_161 {
        let shifted = julian_day + 32_044;
        let century = (4 * shifted + 3).div_euclid(146_097);
        (century, shifted - (146_097 * century).div_euclid(4))
    } else {
        (0, julian_day + 32_082)
    };
    let years = (4 * remaining + 3).div_euclid(1461);
    let day_of_year = remaining - (1461 * years).div_euclid(4);
    let month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month + 2) / 5 + 1;
    (
        100 * century + years - 4800 + month / 10,
        month + 3 - 12 * (month / 10),
        day,
    )
}

impl ScalarUDFImpl for FromUnixTime {
    fn name(&self) -> &str {
        "flink_from_unixtime"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(
            |arrays: &[ArrayRef]| {
                let input = arrays[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution(
                            "FROM_UNIXTIME requires integer seconds".into(),
                        )
                    })?;
                let mut output = StringBuilder::with_capacity(input.len(), input.len() * 20);
                let mut formatted = String::with_capacity(32);
                let mut previous = None;
                for value in input.iter() {
                    match value {
                        None => output.append_null(),
                        Some(value) => {
                            if previous != Some(value) {
                                formatted.clear();
                                self.format_into(value, &mut formatted);
                                previous = Some(value);
                            }
                            output.append_value(&formatted);
                        }
                    }
                }
                Ok(Arc::new(output.finish()) as ArrayRef)
            },
            vec![],
        )(&args.args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::Field;
    use datafusion::common::{config::ConfigOptions, ScalarValue};

    fn evaluate(input: ColumnarValue, rows: usize) -> ColumnarValue {
        function(0, "yyyyMMddHHmm")
            .unwrap()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![input],
                arg_fields: vec![],
                number_rows: rows,
                return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
                config_options: Arc::new(ConfigOptions::new()),
            })
            .unwrap()
    }

    #[test]
    fn scalar_nulls_empty_batches_and_sliced_arrays_keep_their_shape() {
        let ColumnarValue::Scalar(value) =
            evaluate(ColumnarValue::Scalar(ScalarValue::Int64(None)), 0)
        else {
            panic!("scalar")
        };
        assert_eq!(value, ScalarValue::Utf8(None));
        let values = Int64Array::from(vec![Some(123), Some(0), None, Some(-1)]).slice(1, 3);
        let ColumnarValue::Array(output) = evaluate(ColumnarValue::Array(Arc::new(values)), 3)
        else {
            panic!("array")
        };
        assert_eq!(
            output.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec![Some("197001010000"), None, Some("196912312359")])
        );
        let ColumnarValue::Array(output) = evaluate(
            ColumnarValue::Array(Arc::new(Int64Array::from(Vec::<i64>::new()))),
            0,
        ) else {
            panic!("empty array")
        };
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn calendar_cutover_bce_and_wrapping_match_java() {
        let formatter = FromUnixTime {
            offset_millis: 0,
            transitions: vec![],
            tokens: compile("yyyy-MM-dd HH:mm:ss").unwrap(),
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        };
        for (seconds, expected) in [
            (0, "1970-01-01 00:00:00"),
            (-1, "1969-12-31 23:59:59"),
            (-12_219_292_800, "1582-10-15 00:00:00"),
            (-12_219_292_801, "1582-10-04 23:59:59"),
            (-62_135_769_600, "0001-01-01 00:00:00"),
            (-62_135_769_601, "0001-12-31 23:59:59"),
            (i64::MIN, "1970-01-01 00:00:00"),
            (i64::MAX, "1969-12-31 23:59:59"),
        ] {
            assert_eq!(formatter.format(seconds), expected);
        }
    }

    #[test]
    fn quoted_literals_are_compiled_and_unknown_patterns_rejected() {
        assert!(compile("yyyyMMddHHmm 'o''clock' 😀").is_ok());
        assert!(compile("").is_ok());
        for invalid in ["yy", "MMMM", "EEE", "X", "yyyy 'unfinished"] {
            assert!(compile(invalid).is_err());
        }
        let formatter = FromUnixTime {
            offset_millis: 19_800_000,
            transitions: vec![],
            tokens: compile("yyyyMMddHHmm 'o''clock' 😀").unwrap(),
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        };
        assert_eq!(formatter.format(0), "197001010530 o'clock 😀");
    }

    #[test]
    fn encoded_offsets_are_selected_after_wrapping_seconds_to_milliseconds() {
        let formatter = FromUnixTime {
            offset_millis: 28_800_000,
            transitions: vec![(0, 32_400_000), (3_600_000, 28_800_000)],
            tokens: compile("yyyy-MM-dd HH:mm:ss").unwrap(),
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        };
        for (seconds, expected) in [
            (-1, "1970-01-01 07:59:59"),
            (0, "1970-01-01 09:00:00"),
            (3599, "1970-01-01 09:59:59"),
            (3600, "1970-01-01 09:00:00"),
            (i64::MIN, "1970-01-01 09:00:00"),
            (i64::MAX, "1970-01-01 07:59:59"),
        ] {
            assert_eq!(formatter.format(seconds), expected);
        }
        assert!(from_encoded("28800000,0:32400000,3600000:28800000", "yyyyMMddHHmm").is_ok());
        for invalid in ["", "bad", "0,bad", "0,2:0,1:0", "0,1:0,1:0"] {
            assert!(from_encoded(invalid, "yyyyMMddHHmm").is_err());
        }
    }
}
