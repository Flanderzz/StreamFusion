// A connector or format library is this crate without `core`: the crate-wide prelude and the shared
// modules then carry items only the engine uses, which is not a defect of that build.
#![cfg_attr(not(feature = "core"), allow(unused_imports, dead_code))]

pub(crate) use arrow::array::builder::{
    BinaryBuilder, BooleanBuilder, Int64Builder, PrimitiveBuilder, StringBuilder,
};
pub(crate) use arrow::array::types::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampNanosecondType,
};
pub(crate) use arrow::array::NullBufferBuilder;
pub(crate) use arrow::array::{
    make_array, new_empty_array, new_null_array, Array, ArrayRef, BinaryArray, BooleanArray,
    Decimal128Array, DictionaryArray, Float32Array, Int16Array, Int32Array, Int64Array, Int8Array,
    IntervalDayTimeArray, ListArray, MapArray, MutableArrayData, PrimitiveArray, RecordBatch,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, UInt32Array,
};
pub(crate) use arrow::buffer::{OffsetBuffer, ScalarBuffer};
pub(crate) use arrow::compute::{concat_batches, filter_record_batch, take, SortOptions};
pub(crate) use arrow::datatypes::ArrowPrimitiveType;
pub(crate) use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
pub(crate) use arrow::ffi::{from_ffi, from_ffi_and_data_type, FFI_ArrowArray, FFI_ArrowSchema};
pub(crate) use arrow::row::{OwnedRow, Row, RowConverter, Rows, SortField};
pub(crate) use datafusion::catalog::memory::MemorySourceConfig;
pub(crate) use datafusion::common::{DFSchema, DataFusionError, JoinSide, JoinType, NullEquality};
pub(crate) use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
};
pub(crate) use datafusion::execution::runtime_env::RuntimeEnvBuilder;
pub(crate) use datafusion::execution::TaskContext;
pub(crate) use datafusion::functions_aggregate::count::count_udaf;
pub(crate) use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
pub(crate) use datafusion::functions_aggregate::sum::sum_udaf;
pub(crate) use datafusion::logical_expr::execution_props::ExecutionProps;
pub(crate) use datafusion::logical_expr::{Accumulator, AggregateUDF, Operator};
pub(crate) use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
pub(crate) use datafusion::physical_expr::aggregate::{
    AggregateExprBuilder, AggregateFunctionExpr,
};
pub(crate) use datafusion::physical_expr::expressions::{binary, col, lit, Column};
pub(crate) use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
pub(crate) use datafusion::physical_plan::collect;
pub(crate) use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
pub(crate) use datafusion::physical_plan::joins::{HashJoinExec, JoinOn, PartitionMode};
pub(crate) use datafusion::prelude::{col as logical_col, lit as logical_lit, SessionContext};
pub(crate) use datafusion::scalar::ScalarValue;
pub(crate) use futures::StreamExt;
pub(crate) use jni::objects::{
    JByteArray, JClass, JDoubleArray, JFloatArray, JIntArray, JLongArray, JObjectArray, JString,
};
pub(crate) use jni::sys::{jboolean, jbyteArray, jint, jlong, jstring};
pub(crate) use jni::JNIEnv;
// ahash, not std's SipHash: every keyed hot loop in the crate hashes through these aliases, and
// the CPU profiles showed SipHash as a top cost wherever an operator missed the explicit swap
// (q18's keep-last dedup spent ~35% of its time in it). DoS-hardness is irrelevant for internal
// operator state, so the fast hash is the right crate-wide default.
pub(crate) use ahash::{HashMap, HashSet};
pub(crate) use std::collections::BTreeMap;
pub(crate) use std::sync::{Arc, Mutex, OnceLock};
pub(crate) use tokio::runtime::Runtime;

// The engine modules ride the `core` feature; a connector or format library leaves it off and
// compiles only the shared modules below it plus its own.
#[cfg(any(feature = "core", test))]
mod aggregates;
mod avro;
mod avro_datum;
mod bridge;
#[cfg(any(feature = "core", test))]
mod bucket_route;
#[cfg(any(feature = "core", test))]
mod calc;
mod changelog;
#[cfg(any(
    feature = "json",
    feature = "csv",
    feature = "raw",
    feature = "avro",
    feature = "protobuf",
    test
))]
mod csv;
// The sink-side CSV encoder: it needs the Kafka sink's encode seam (EncodedLines and the shared
// Flink text helpers live with it), and rides the `csv` feature so a connector build without the
// format compiles the dispatch's unsupported arm instead.
#[cfg(all(feature = "kafka", feature = "csv"))]
mod csv_encode;
#[cfg(any(feature = "core", test))]
mod dedup;
#[cfg(any(feature = "core", test))]
mod exchange;
#[cfg(any(feature = "core", test))]
mod expr;
#[cfg(feature = "parquet")]
mod files;
#[cfg(any(feature = "core", test))]
mod flatten;
#[cfg(any(feature = "core", test))]
mod flink_functions;
#[cfg(any(feature = "core", test))]
mod flink_key;
#[cfg(any(
    feature = "json",
    feature = "csv",
    feature = "raw",
    feature = "avro",
    feature = "protobuf",
    test
))]
mod flink_text;
mod format_abi;
mod format_codes;
#[cfg(any(
    feature = "json",
    feature = "csv",
    feature = "raw",
    feature = "avro",
    feature = "protobuf",
    test
))]
mod formats;
#[cfg(any(feature = "core", test))]
mod group_agg;
#[cfg(any(feature = "core", test))]
mod interval_join;
#[cfg(any(feature = "core", test))]
mod ipc;
mod jdk_decimal;
mod jdk_double;
#[cfg(any(feature = "core", test))]
mod join_common;
#[cfg(any(
    feature = "json",
    feature = "csv",
    feature = "raw",
    feature = "avro",
    feature = "protobuf",
    test
))]
mod json;
#[cfg(any(
    feature = "json",
    feature = "csv",
    feature = "raw",
    feature = "avro",
    feature = "protobuf",
    test
))]
mod json_retry;
#[cfg(feature = "kafka")]
mod kafka;
#[cfg(any(feature = "core", test))]
mod keyed_upsert;
#[cfg(any(feature = "core", test))]
mod keys;
#[cfg(any(feature = "core", test))]
mod logging;
#[cfg(any(feature = "core", test))]
mod memory;
#[cfg(any(feature = "core", test))]
mod mini_batch;
#[cfg(any(feature = "core", test))]
mod normalizer;
#[cfg(any(feature = "core", test))]
mod over_agg;
#[cfg(any(feature = "protobuf", test))]
mod protobuf_decode;
#[cfg(any(feature = "protobuf", test))]
mod protobuf_encode;
#[cfg(any(feature = "raw", test))]
mod raw_encode;
#[cfg(any(feature = "core", test))]
mod rowtime;
#[cfg(any(feature = "core", test))]
mod session_agg;
#[cfg(any(feature = "core", test))]
mod sorter;
#[cfg(any(feature = "core", test))]
mod state;
#[cfg(any(feature = "core", test))]
mod temporal_join;
#[cfg(any(feature = "core", test))]
mod topn;
#[cfg(any(feature = "core", test))]
mod updating_join;
#[cfg(any(feature = "core", test))]
mod window_agg;
#[cfg(any(feature = "core", test))]
mod window_join;

// Flatten the crate namespace: every module starts with `use crate::*;`, so items cross module
// boundaries (and reach the tests via `super::*`) without per-module import lists. A self-contained
// operator's re-export is "unused" outside `cfg(test)`, hence the allow.
#[allow(unused_imports)]
pub(crate) use {
    bridge::*, changelog::*, format_abi::*, format_codes::*, jdk_decimal::*, jdk_double::*,
};

#[cfg(any(feature = "core", test))]
#[allow(unused_imports)]
pub(crate) use {
    aggregates::*, calc::*, dedup::*, exchange::*, expr::*, flatten::*, flink_key::*, group_agg::*,
    interval_join::*, ipc::*, join_common::*, keyed_upsert::*, keys::*, memory::*, mini_batch::*,
    normalizer::*, over_agg::*, rowtime::*, session_agg::*, sorter::*, state::*, temporal_join::*,
    topn::*, updating_join::*, window_agg::*, window_join::*,
};

#[cfg(any(
    feature = "json",
    feature = "csv",
    feature = "raw",
    feature = "avro",
    feature = "protobuf",
    test
))]
#[allow(unused_imports)]
pub(crate) use {formats::*, json::*};

#[cfg(any(feature = "protobuf", test))]
#[allow(unused_imports)]
pub(crate) use protobuf_encode::*;

#[cfg(any(feature = "raw", test))]
#[allow(unused_imports)]
pub(crate) use raw_encode::*;

#[cfg(feature = "parquet")]
#[allow(unused_imports)]
pub(crate) use files::*;

#[cfg(feature = "kafka")]
pub(crate) use kafka::*;

/// Thin wrappers exposing the engine hot paths to the Criterion benchmark harness, without leaking
/// the JNI internals or the Arrow-FFI plumbing. Not used by the JVM bridge.
#[cfg(any(feature = "core", test))]
pub mod bench;

#[cfg(test)]
mod tests;
