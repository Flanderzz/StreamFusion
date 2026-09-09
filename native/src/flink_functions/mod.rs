//! Flink scalar registrations and kernels for semantics that differ from DataFusion.

use datafusion::logical_expr::ScalarUDF;

pub(crate) fn function(_op: i64, _arity: usize) -> Option<ScalarUDF> {
    None
}
