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
