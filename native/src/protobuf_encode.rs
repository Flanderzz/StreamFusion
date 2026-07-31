//! Arrow → protobuf encoder for the Kafka sink's `protobuf` format, replicating Flink's
//! `PbCodegenRowSerializer`/`PbCodegenUtils` semantics over the field shapes the planner's
//! descriptor gate admits (proto3, no explicit-presence scalars, signed ints / float / double /
//! bool / string leaves, nested messages, repeated fields, maps).
//!
//! Contract: batch columns map to proto fields **by name** (order-independent), exactly the
//! decoder's schema derivation in reverse; a Struct column's children map to the nested message's
//! fields by name, recursively. Every column must name a field of the message — the sink wiring
//! owns projection (including stripping any changelog column), so an unmatched column is a bug and
//! panics. Fields no column names stay unset. The batch is assumed insert-only.
//!
//! Null semantics, mirroring Flink's serializer exactly:
//! - A null column (at any row-nesting level) leaves the proto field unset — Flink guards every
//!   field with `if(!rowData.isNullAt(i))` (PbCodegenRowSerializer#codegen), so a null ARRAY/MAP/
//!   ROW column is never touched, and a null field inside a non-null nested row leaves the nested
//!   builder's field unset.
//! - Nulls INSIDE containers (protobuf forbids them) become type defaults — 0 / 0.0 / false, the
//!   `protobuf.write-null-string-literal` value for strings, the default (empty) instance for
//!   messages — for array elements and for both map keys and map values
//!   (PbCodegenUtils#pbDefaultValueCode + #convertFlinkArrayElementToPbWithDefaultValueCode,
//!   reached from both PbCodegenArraySerializer and PbCodegenMapSerializer).
//!
//! Correctness-first: each row builds a `prost-reflect` `DynamicMessage` (which owns the
//! sint/sfixed wire encodings and proto3 default-skipping from the descriptor). Encoding the wire
//! format directly from the Arrow columns, without the per-row message, is a later optimization.

use crate::*;

use arrow::array::Float64Array;
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor, Value,
};

/// One bare serialized protobuf message per row, all in a single encode buffer (the JSON sink's
/// `EncodedLines` shape): producing and JNI materialization read the per-row slices in place.
/// Rows stay 1:1 with the input batch — a row with every field unset is a zero-length slice,
/// the same empty `byte[]` Flink's serializer produces for it.
pub(crate) struct EncodedMessages {
    bytes: Vec<u8>,
    rows: Vec<std::ops::Range<usize>>,
}

#[allow(dead_code)] // consumed by the sink format dispatch when the wiring pass lands
impl EncodedMessages {
    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(crate) fn message(&self, index: usize) -> &[u8] {
        &self.bytes[self.rows[index].clone()]
    }
}

pub(crate) struct ProtobufEncoder {
    message: MessageDescriptor,
    null_string_literal: String,
}

#[allow(dead_code)] // consumed by the sink format dispatch when the wiring pass lands
impl ProtobufEncoder {
    /// `descriptor_set` is an encoded protobuf `FileDescriptorSet` (the message's file + its
    /// transitive dependencies); `message_name` is the fully-qualified message type each row
    /// serializes as; `null_string_literal` is Flink's `protobuf.write-null-string-literal`
    /// option (default ""), substituted for null strings inside containers.
    pub(crate) fn new(
        descriptor_set: &[u8],
        message_name: &str,
        null_string_literal: &str,
    ) -> ProtobufEncoder {
        let pool = DescriptorPool::decode(descriptor_set)
            .expect("failed to decode protobuf FileDescriptorSet");
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("protobuf message {message_name} not found in descriptor"));
        ProtobufEncoder { message, null_string_literal: null_string_literal.to_string() }
    }

    pub(crate) fn encode(&self, batch: &RecordBatch) -> EncodedMessages {
        use prost_reflect::prost::Message as _;
        let fields = batch.schema_ref().fields().clone();
        let mut bytes = Vec::new();
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let start = bytes.len();
            self.message_at(&self.message, &fields, batch.columns(), row)
                .encode(&mut bytes)
                .expect("failed to encode protobuf message");
            rows.push(start..bytes.len());
        }
        EncodedMessages { bytes, rows }
    }

    fn message_at(
        &self,
        descriptor: &MessageDescriptor,
        fields: &Fields,
        columns: &[ArrayRef],
        row: usize,
    ) -> DynamicMessage {
        let mut message = DynamicMessage::new(descriptor.clone());
        for (field, column) in fields.iter().zip(columns) {
            if column.is_null(row) {
                continue;
            }
            let proto_field = descriptor.get_field_by_name(field.name()).unwrap_or_else(|| {
                panic!(
                    "column {} names no field of protobuf message {}",
                    field.name(),
                    descriptor.full_name()
                )
            });
            message.set_field(&proto_field, self.field_value(&proto_field, column, row));
        }
        message
    }

    fn field_value(&self, field: &FieldDescriptor, column: &ArrayRef, row: usize) -> Value {
        if field.is_map() {
            return self.map_value(field, column, row);
        }
        if field.is_list() {
            let list = typed::<ListArray>(column, &field.kind());
            let elements = list.value(row);
            let values =
                (0..elements.len()).map(|i| self.element_value(&field.kind(), &elements, i));
            return Value::List(values.collect());
        }
        self.leaf_value(&field.kind(), column, row)
    }

    fn map_value(&self, field: &FieldDescriptor, column: &ArrayRef, row: usize) -> Value {
        let Kind::Message(entry) = field.kind() else {
            panic!("protobuf map field {} has a non-message entry kind", field.name())
        };
        let key_kind = entry.map_entry_key_field().kind();
        let value_kind = entry.map_entry_value_field().kind();
        let map = typed::<MapArray>(column, &field.kind());
        let entries = map.value(row);
        let (keys, values) = (entries.column(0), entries.column(1));
        let pairs = (0..entries.len())
            .map(|i| (self.key_value(&key_kind, keys, i), self.element_value(&value_kind, values, i)));
        Value::Map(pairs.collect())
    }

    /// A container slot: protobuf forbids null elements, so a null substitutes the type default,
    /// exactly Flink's `convertFlinkArrayElementToPbWithDefaultValueCode`.
    fn element_value(&self, kind: &Kind, array: &ArrayRef, index: usize) -> Value {
        if array.is_null(index) {
            return self.default_value(kind);
        }
        self.leaf_value(kind, array, index)
    }

    fn leaf_value(&self, kind: &Kind, array: &ArrayRef, index: usize) -> Value {
        match kind {
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
                Value::I32(typed::<Int32Array>(array, kind).value(index))
            }
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
                Value::I64(typed::<Int64Array>(array, kind).value(index))
            }
            Kind::Float => Value::F32(typed::<Float32Array>(array, kind).value(index)),
            Kind::Double => Value::F64(typed::<Float64Array>(array, kind).value(index)),
            Kind::Bool => Value::Bool(typed::<BooleanArray>(array, kind).value(index)),
            Kind::String => Value::String(typed::<StringArray>(array, kind).value(index).to_string()),
            Kind::Message(descriptor) => {
                let strukt = typed::<StructArray>(array, kind);
                Value::Message(self.message_at(descriptor, strukt.fields(), strukt.columns(), index))
            }
            unsupported => panic!("{}", outside_gate(unsupported)),
        }
    }

    fn key_value(&self, kind: &Kind, array: &ArrayRef, index: usize) -> MapKey {
        if array.is_null(index) {
            return match kind {
                Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => MapKey::I32(0),
                Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => MapKey::I64(0),
                Kind::Bool => MapKey::Bool(false),
                Kind::String => MapKey::String(self.null_string_literal.clone()),
                unsupported => panic!("{}", outside_gate(unsupported)),
            };
        }
        match kind {
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
                MapKey::I32(typed::<Int32Array>(array, kind).value(index))
            }
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
                MapKey::I64(typed::<Int64Array>(array, kind).value(index))
            }
            Kind::Bool => MapKey::Bool(typed::<BooleanArray>(array, kind).value(index)),
            Kind::String => MapKey::String(typed::<StringArray>(array, kind).value(index).to_string()),
            unsupported => panic!("{}", outside_gate(unsupported)),
        }
    }

    /// Flink's `PbCodegenUtils#pbDefaultValueCode`: the substitute for a null container element —
    /// numeric zero, false, the write-null-string-literal, or a message's default (empty) instance.
    fn default_value(&self, kind: &Kind) -> Value {
        match kind {
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => Value::I32(0),
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => Value::I64(0),
            Kind::Float => Value::F32(0.0),
            Kind::Double => Value::F64(0.0),
            Kind::Bool => Value::Bool(false),
            Kind::String => Value::String(self.null_string_literal.clone()),
            Kind::Message(descriptor) => Value::Message(DynamicMessage::new(descriptor.clone())),
            unsupported => panic!("{}", outside_gate(unsupported)),
        }
    }
}

fn typed<'a, T: Array + 'static>(array: &'a ArrayRef, kind: &Kind) -> &'a T {
    array.as_any().downcast_ref::<T>().unwrap_or_else(|| {
        panic!(
            "protobuf {kind:?} field cannot encode from an Arrow {:?} column",
            array.data_type()
        )
    })
}

fn outside_gate(kind: &Kind) -> String {
    format!(
        "protobuf field kind {kind:?} is outside the native encode gate \
         (proto3 signed ints, float, double, bool, string, nested messages)"
    )
}
