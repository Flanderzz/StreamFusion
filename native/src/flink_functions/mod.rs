//! Flink scalar registrations and kernels for semantics that differ from DataFusion.

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{
    DataType, Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use datafusion::common::{cast::as_primitive_array, exec_err, Result};
use datafusion::logical_expr::{ScalarUDF, Volatility};
use std::sync::Arc;

mod binary_strings;
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
