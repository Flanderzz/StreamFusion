//! Flink scalar registrations and kernels for semantics that differ from DataFusion.

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{
    DataType, Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use datafusion::common::{cast::as_primitive_array, exec_err, Result};
use datafusion::logical_expr::{ScalarUDF, Volatility};
use std::sync::Arc;

pub(crate) fn function(op: i64, arity: usize) -> Option<ScalarUDF> {
    Some(match op {
        100 => datafusion::functions::string::starts_with()
            .as_ref()
            .clone(),
        101 => datafusion::functions::string::ends_with().as_ref().clone(),
        _ => return None,
    })
}
