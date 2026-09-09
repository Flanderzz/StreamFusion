//! Flink scalar registrations and kernels for semantics that differ from DataFusion.

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{
    DataType, Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use datafusion::common::{cast::as_primitive_array, exec_err, Result};
use datafusion::logical_expr::{ScalarUDF, Volatility};
use std::sync::Arc;

pub(crate) mod decode;
pub(crate) mod encode;
pub(crate) mod json_quote;
pub(crate) mod json_unquote;
pub(crate) mod split;
pub(crate) mod substring;

mod binary_strings;
mod charset;
mod locate;
mod scalar;

const HEX_DIGITS: &[u8; 16] = b"0123456789ABCDEF";

pub(crate) fn function(op: i64, arity: usize) -> Option<ScalarUDF> {
    Some(match op {
        100 => datafusion::functions::string::starts_with()
            .as_ref()
            .clone(),
        101 => datafusion::functions::string::ends_with().as_ref().clone(),
        102 => datafusion::functions::unicode::strpos().as_ref().clone(),
        103 => ScalarUDF::new_from_impl(locate::FlinkLocate::new()),
        104 => udf(
            "flink_bin",
            vec![DataType::Int64],
            DataType::Utf8,
            binary_strings::bin,
        ),
        105 => udf(
            "flink_hex_int",
            vec![DataType::Int64],
            DataType::Utf8,
            binary_strings::hex_int,
        ),
        106 => udf(
            "flink_hex_string",
            vec![DataType::Utf8],
            DataType::Utf8,
            |args| binary_strings::encode(args, false),
        ),
        107 => udf(
            "flink_to_base64",
            vec![DataType::Utf8],
            DataType::Utf8,
            |args| binary_strings::encode(args, true),
        ),
        108 => udf(
            "flink_unhex",
            vec![DataType::Utf8],
            DataType::Binary,
            binary_strings::unhex,
        ),
        109 => scalar::extremum(true),
        110 => scalar::extremum(false),
        111 => udf(
            "flink_initcap",
            vec![DataType::Utf8],
            DataType::Utf8,
            scalar::initcap,
        ),
        112 => udf(
            "flink_translate",
            vec![DataType::Utf8; 3],
            DataType::Utf8,
            scalar::translate,
        ),
        113 => datafusion::functions::string::btrim().as_ref().clone(),
        114 => scalar::elt_function(arity),
        115 => udf(
            "flink_url_encode",
            vec![DataType::Utf8],
            DataType::Utf8,
            scalar::url_encode,
        ),
        116 => {
            let mut types = vec![DataType::Int64; arity];
            for datatype in types.iter_mut().take(2) {
                *datatype = DataType::Utf8;
            }
            udf("flink_overlay", types, DataType::Utf8, scalar::overlay)
        }
        117 => udf(
            "flink_url_decode",
            vec![DataType::Utf8],
            DataType::Utf8,
            scalar::url_decode,
        ),
        118 => udf(
            "flink_url_decode_ascii",
            vec![DataType::Utf8],
            DataType::Utf8,
            scalar::url_decode_ascii,
        ),
        120 => encode::function(),
        121 => decode::function(),
        122 => json_quote::function(),
        123 => json_unquote::function(),
        124 => split::function(),
        125 => substring::function(arity),
        _ => return None,
    })
}

fn udf(
    name: &str,
    inputs: Vec<DataType>,
    output: DataType,
    kernel: fn(&[ArrayRef]) -> Result<ArrayRef>,
) -> ScalarUDF {
    datafusion::logical_expr::create_udf(
        name,
        inputs,
        output,
        Volatility::Immutable,
        Arc::new(datafusion::functions::utils::make_scalar_function(
            kernel,
            vec![],
        )),
    )
}

fn check_string_capacity(current: usize, additional: usize) -> Result<()> {
    if current
        .checked_add(additional)
        .is_none_or(|size| size > i32::MAX as usize)
    {
        return exec_err!("Function output exceeds Arrow string capacity");
    }
    Ok(())
}
