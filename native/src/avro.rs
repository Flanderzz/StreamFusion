use crate::*;
use arrow::array::types::{ArrowTimestampType, TimestampMicrosecondType, TimestampMillisecondType};
use arrow::array::PrimitiveArray;
use arrow::compute::kernels::arity::unary;
use arrow::datatypes::TimeUnit;
use arrow_avro::schema::{AvroSchema, Fingerprint, FingerprintAlgorithm, SchemaStore};

/// Decodes Avro message bodies through arrow-avro — bare datums (Flink's `avro`) or
/// Confluent-framed ones (`avro-confluent`) — and reconciles the decoded batch with the Arrow
/// boundary schema the operators expect. arrow-avro derives its own Arrow types from the Avro
/// schema, which differ from the boundary's conventions (timestamp units, small ints, nested child
/// field names); [`reconcile`] closes that gap so the rest of the pipeline never sees an
/// avro-shaped batch.
pub(crate) struct AvroDecoder {
    store: SchemaStore,
    reader: Option<AvroSchema>,
    /// The boundary schema the JVM exported from the table's row type. Empty for the
    /// benchmark-only counting path, which skips reconciliation.
    target: SchemaRef,
    /// Bare datums (Flink's `avro`): the one writer schema sits at synthetic id 0 and each message
    /// gets the 5-byte id-0 Confluent header prepended so the framed decoder applies.
    bare: bool,
}

/// An arrow-avro writer store keyed by integer id (the Confluent / id-framing layout). An empty
/// schema string builds an empty store — the Confluent path starts with no writer schemas and feeds
/// them in by id as the JVM fetches them from the schema registry (`registerAvroSchema`).
fn store(avro_schema: &str, id: u32) -> SchemaStore {
    let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
    if !avro_schema.is_empty() {
        store
            .set(Fingerprint::Id(id), AvroSchema::new(avro_schema.to_string()))
            .expect("failed to register avro schema");
    }
    store
}

impl AvroDecoder {
    pub(crate) fn confluent(
        avro_schema: &str,
        schema_id: u32,
        reader: Option<AvroSchema>,
        target: SchemaRef,
    ) -> AvroDecoder {
        AvroDecoder { store: store(avro_schema, schema_id), reader, target, bare: false }
    }

    pub(crate) fn bare(
        avro_schema: &str,
        reader: Option<AvroSchema>,
        target: SchemaRef,
    ) -> AvroDecoder {
        AvroDecoder { store: store(avro_schema, 0), reader, target, bare: true }
    }

    /// Registers a writer schema under a Confluent schema id, so subsequent decodes resolve
    /// messages framed with that id. Only the Confluent variant carries an id-keyed store.
    pub(crate) fn register_writer_schema(&mut self, id: u32, schema: &str) {
        assert!(!self.bare, "registerAvroSchema on a bare-avro decoder");
        self.store
            .set(Fingerprint::Id(id), AvroSchema::new(schema.to_string()))
            .expect("failed to register avro schema");
    }

    /// Decodes a binary "body" batch into typed Arrow against the local schema-id store. A null
    /// body contributes no row — Flink's deserializer returns null for a null Kafka value (a
    /// tombstone), which the collector drops silently.
    pub(crate) fn decode(&self, body: &RecordBatch) -> RecordBatch {
        let column = body.column(0).as_any().downcast_ref::<BinaryArray>().expect("binary body");
        let build = || {
            let mut builder = arrow_avro::reader::ReaderBuilder::new()
                .with_writer_schema_store(self.store.clone())
                .with_batch_size(column.len().max(1));
            // With a reader schema, Avro resolution decodes the full writer datum but materializes
            // only the reader's (subset of) fields — projection pushed into the decode. Writer
            // fields the reader omits are parsed and discarded, never built into Arrow.
            if let Some(reader_schema) = &self.reader {
                builder = builder.with_reader_schema(reader_schema.clone());
            }
            builder.build_decoder().expect("failed to build avro decoder")
        };
        // Built on the first surviving body: an all-tombstone batch must decode to zero rows even
        // before any writer schema has been registered (arrow-avro refuses an empty store).
        let mut decoder = None;
        let mut framed = Vec::new();
        // A message framed with a different schema id than its predecessor makes the decoder stop
        // consuming until the rows decoded so far are flushed (it can't mix writer schemas in one
        // build), so decode in a loop, flushing whenever a message is only partially consumed. With
        // a reader schema every flushed batch has the same (reader) shape, so the flushes
        // concatenate.
        let mut batches = Vec::new();
        for i in 0..column.len() {
            if !column.is_valid(i) {
                continue;
            }
            if column.value(i).is_empty() {
                // Flink's plain avro/avro-confluent deserializers hit EOF on an empty body and
                // fail the job; silently dropping it would diverge.
                panic!("avro decode failed: empty message body");
            }
            let bytes = if self.bare {
                framed.clear();
                framed.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00]); // id-0 Confluent header
                framed.extend_from_slice(column.value(i));
                &framed[..]
            } else {
                column.value(i)
            };
            let decoder = decoder.get_or_insert_with(build);
            let mut consumed = 0;
            while consumed < bytes.len() {
                let n = decoder.decode(&bytes[consumed..]).expect("avro decode failed");
                consumed += n;
                if consumed < bytes.len() {
                    match decoder.flush().expect("avro flush failed") {
                        Some(batch) => batches.push(batch),
                        // No progress and nothing to flush: the message is truncated/malformed.
                        None if n == 0 => panic!("avro decode stalled on a malformed message"),
                        None => {}
                    }
                }
            }
        }
        if let Some(batch) = decoder.as_mut().and_then(|d| d.flush().expect("avro flush failed")) {
            batches.push(batch);
        }
        if self.target.fields().is_empty() {
            // Benchmark-only counting path: no boundary schema, so no reconciliation.
            return match batches.len() {
                0 => panic!("an all-null avro body batch needs the boundary schema"),
                1 => batches.into_iter().next().unwrap(),
                _ => {
                    let schema = batches[0].schema();
                    concat_batches(&schema, &batches).expect("avro batch concat failed")
                }
            };
        }
        // Reconcile each flush before concatenating: writer schemas differing mid-batch can flush
        // under reader shapes that differ in field metadata (arrow-avro annotates a defaulted
        // field), and reconciliation lands every flush on the one boundary schema.
        let mut reconciled = batches.into_iter().map(|batch| reconcile(&self.target, batch));
        match (reconciled.next(), reconciled.next()) {
            (None, _) => RecordBatch::new_empty(self.target.clone()),
            (Some(single), None) => single,
            (Some(first), Some(second)) => {
                let batches: Vec<RecordBatch> =
                    [first, second].into_iter().chain(reconciled).collect();
                concat_batches(&self.target, &batches).expect("avro batch concat failed")
            }
        }
    }
}

/// Rebuilds a decoded batch onto the boundary schema. arrow-avro's Arrow mapping is faithful to the
/// Avro logical types; Flink's converters are not, and parity means reproducing Flink:
///
/// - Every avro timestamp long is epoch *milliseconds* to Flink regardless of the schema's declared
///   unit — `AvroToRowDataConverters` reads the raw long with `fromEpochMillis` even for a
///   `*-timestamp-micros` schema — so every source unit scales by 1e6 to the boundary's nanoseconds.
/// - TINYINT/SMALLINT narrow from the avro int with Java's wrapping `byteValue()`/`shortValue()`.
/// - A decimal whose digits exceed the declared precision is NULL (`DecimalData.fromBigDecimal`).
/// - Nested arrays/maps/structs are rebuilt onto the boundary's child fields (arrow-avro names a
///   list child `item` and map entries `entries`; the boundary uses `element`/`items`).
fn reconcile(target: &SchemaRef, batch: RecordBatch) -> RecordBatch {
    let columns = target
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| reconcile_array(field, column.clone()))
        .collect();
    RecordBatch::try_new(target.clone(), columns)
        .expect("decoded avro batch does not fit the boundary schema")
}

fn reconcile_array(field: &Field, array: ArrayRef) -> ArrayRef {
    if let DataType::Decimal128(precision, scale) = field.data_type() {
        return reconcile_decimal(array, *precision, *scale);
    }
    if field.data_type() == array.data_type() {
        return array;
    }
    match field.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, None) => flink_timestamp_nanos(&array),
        DataType::Int8 => {
            let ints = array.as_any().downcast_ref::<Int32Array>().expect("avro int for TINYINT");
            Arc::new(unary::<Int32Type, _, Int8Type>(ints, |v| v as i8))
        }
        DataType::Int16 => {
            let ints = array.as_any().downcast_ref::<Int32Array>().expect("avro int for SMALLINT");
            Arc::new(unary::<Int32Type, _, Int16Type>(ints, |v| v as i16))
        }
        DataType::List(element) => {
            let list = array.as_any().downcast_ref::<ListArray>().expect("avro array");
            let (_, offsets, values, nulls) = list.clone().into_parts();
            Arc::new(ListArray::new(
                element.clone(),
                offsets,
                reconcile_array(element, values),
                nulls,
            ))
        }
        DataType::Struct(children) => {
            let source = array.as_any().downcast_ref::<StructArray>().expect("avro record");
            let columns = children
                .iter()
                .zip(source.columns())
                .map(|(child, column)| reconcile_array(child, column.clone()))
                .collect();
            Arc::new(StructArray::new(children.clone(), columns, source.nulls().cloned()))
        }
        DataType::Map(entries, sorted) => {
            let source = array.as_any().downcast_ref::<MapArray>().expect("avro map");
            let DataType::Struct(children) = entries.data_type() else {
                panic!("map entries are not a struct")
            };
            let key = reconcile_array(&children[0], source.keys().clone());
            let value = reconcile_array(&children[1], source.values().clone());
            let struct_entries = StructArray::new(children.clone(), vec![key, value], None);
            Arc::new(MapArray::new(
                entries.clone(),
                source.offsets().clone(),
                struct_entries,
                source.nulls().cloned(),
                *sorted,
            ))
        }
        other => arrow::compute::cast(&array, other).unwrap_or_else(|e| {
            panic!("avro decode produced {} where the boundary needs {other}: {e}", array.data_type())
        }),
    }
}

/// See [`reconcile`]: the raw stored long is epoch millis to Flink whatever the avro unit says.
fn flink_timestamp_nanos(array: &ArrayRef) -> ArrayRef {
    fn scale<T: ArrowTimestampType>(array: &ArrayRef) -> ArrayRef {
        let raw = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        Arc::new(unary::<T, _, TimestampNanosecondType>(raw, |v| v.wrapping_mul(1_000_000)))
    }
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Millisecond, _) => scale::<TimestampMillisecondType>(array),
        DataType::Timestamp(TimeUnit::Microsecond, _) => scale::<TimestampMicrosecondType>(array),
        other => panic!("avro decode produced {other} for a timestamp column"),
    }
}

/// The reader schema pins the decoded type to `Decimal128(p, s)` (Flink caps precision at 38), but
/// arrow-avro does not validate the unscaled value against the precision; Flink NULLs a value whose
/// digits exceed it (`DecimalData.fromBigDecimal`).
fn reconcile_decimal(array: ArrayRef, precision: u8, scale: i8) -> ArrayRef {
    let expected = DataType::Decimal128(precision, scale);
    let array = if array.data_type() == &expected {
        array
    } else {
        arrow::compute::cast(&array, &expected).expect("avro decimal does not fit Decimal128")
    };
    let decimals = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
    let bound = 10i128.pow(precision as u32);
    if decimals.iter().flatten().all(|v| v.abs() < bound) {
        return array;
    }
    let bounded: Decimal128Array = decimals.iter().map(|v| v.filter(|v| v.abs() < bound)).collect();
    Arc::new(bounded.with_precision_and_scale(precision, scale).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Date32Array, Time32MillisecondArray};

    fn zigzag(n: i64) -> Vec<u8> {
        let mut zz = ((n << 1) ^ (n >> 63)) as u64;
        let mut out = Vec::new();
        loop {
            let mut b = (zz & 0x7f) as u8;
            zz >>= 7;
            if zz != 0 {
                b |= 0x80;
            }
            out.push(b);
            if zz == 0 {
                break;
            }
        }
        out
    }

    fn avro_bytes(bytes: &[u8]) -> Vec<u8> {
        let mut out = zigzag(bytes.len() as i64);
        out.extend_from_slice(bytes);
        out
    }

    fn avro_string(s: &str) -> Vec<u8> {
        avro_bytes(s.as_bytes())
    }

    fn bodies(messages: Vec<Option<&[u8]>>) -> RecordBatch {
        let array = BinaryArray::from(messages);
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("body", DataType::Binary, true)])),
            vec![Arc::new(array)],
        )
        .unwrap()
    }

    fn timestamp_ns(name: &str) -> Field {
        Field::new(name, DataType::Timestamp(TimeUnit::Nanosecond, None), true)
    }

    // The boundary schema for the full reconciled scalar family plus the nested renames: the list
    // child is `element` (arrow-avro emits `item`) and the map entries struct is `items`
    // (arrow-avro emits `entries`).
    fn boundary_schema() -> SchemaRef {
        let map_entries = Field::new(
            "items",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ])),
            false,
        );
        Arc::new(Schema::new(vec![
            Field::new("ti", DataType::Int8, true),
            timestamp_ns("ts"),
            timestamp_ns("tsu"),
            Field::new("dec", DataType::Decimal128(5, 2), true),
            Field::new("d", DataType::Date32, true),
            Field::new("t", DataType::Time32(TimeUnit::Millisecond), true),
            Field::new(
                "arr",
                DataType::List(Arc::new(Field::new("element", DataType::Int64, true))),
                true,
            ),
            Field::new("m", DataType::Map(Arc::new(map_entries), false), true),
        ]))
    }

    const BOUNDARY_WRITER: &str = r#"{"type":"record","name":"Row","fields":[
        {"name":"ti","type":"int"},
        {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
        {"name":"tsu","type":{"type":"long","logicalType":"local-timestamp-micros"}},
        {"name":"dec","type":{"type":"bytes","logicalType":"decimal","precision":5,"scale":2}},
        {"name":"d","type":{"type":"int","logicalType":"date"}},
        {"name":"t","type":{"type":"int","logicalType":"time-millis"}},
        {"name":"arr","type":{"type":"array","items":["null","long"]}},
        {"name":"m","type":{"type":"map","values":["null","long"]}}]}"#;

    fn boundary_datum(ti: i64, millis: i64, unscaled_decimal: &[u8]) -> Vec<u8> {
        let mut datum = zigzag(ti); // int ti
        datum.extend(zigzag(millis)); // ts
        datum.extend(zigzag(millis * 1000)); // tsu (a genuine micros writer)
        datum.extend(avro_bytes(unscaled_decimal)); // dec
        datum.extend(zigzag(19_000)); // d
        datum.extend(zigzag(45_296_789)); // t
        datum.extend(zigzag(1)); // arr: one block of one item
        datum.extend(zigzag(1)); // union branch 1 = long
        datum.extend(zigzag(7));
        datum.extend(zigzag(0)); // arr terminator
        datum.extend(zigzag(1)); // m: one block of one entry
        datum.extend(avro_string("a"));
        datum.extend(zigzag(1)); // union branch 1 = long
        datum.extend(zigzag(5));
        datum.extend(zigzag(0)); // m terminator
        datum
    }

    #[test]
    fn reconciles_decoded_batch_onto_the_boundary_schema() {
        let target = boundary_schema();
        let decoder = AvroDecoder::bare(BOUNDARY_WRITER, None, target.clone());
        // ti=300 wraps to 44 (Java byteValue); dec row 1 = 123.45, row 2 overflows DECIMAL(5,2).
        let m1 = boundary_datum(300, 1_000, &[0x30, 0x39]); // 12345
        let m2 = boundary_datum(-1, -1, &[0x01, 0xE2, 0x40]); // 123456: 6 digits > precision 5
        let out = decoder.decode(&bodies(vec![Some(&m1), Some(&m2)]));

        assert_eq!(out.schema(), target);
        let ti = out.column(0).as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(ti.values(), &[44, -1]);
        let ts = out.column(1).as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
        assert_eq!(ts.values(), &[1_000_000_000, -1_000_000]);
        // Flink reads the micros long as epoch millis (its converter has no micros path); the
        // reconciliation reproduces that: raw x 1e6, not x 1e3.
        let tsu = out.column(2).as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
        assert_eq!(tsu.values(), &[1_000_000_000_000, -1_000_000_000]);
        let dec = out.column(3).as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!((dec.value(0), dec.is_null(1)), (12345, true));
        let d = out.column(4).as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(d.values(), &[19_000, 19_000]);
        let t = out.column(5).as_any().downcast_ref::<Time32MillisecondArray>().unwrap();
        assert_eq!(t.values(), &[45_296_789, 45_296_789]);
        let arr = out.column(6).as_any().downcast_ref::<ListArray>().unwrap();
        let first = arr.value(0);
        assert_eq!(first.as_any().downcast_ref::<Int64Array>().unwrap().values(), &[7]);
        let m = out.column(7).as_any().downcast_ref::<MapArray>().unwrap();
        let keys = m.keys().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!((keys.value(0), keys.value(1)), ("a", "a"));
    }

    // Flink's deserializer returns null for a null Kafka value and the collector drops it — an
    // all-tombstone batch must decode to zero rows, not fail the task.
    #[test]
    fn all_null_bodies_decode_to_an_empty_boundary_batch() {
        let target = boundary_schema();
        let decoder = AvroDecoder::bare(BOUNDARY_WRITER, None, target.clone());
        let out = decoder.decode(&bodies(vec![None, None]));
        assert_eq!((out.num_rows(), out.schema()), (0, target));
    }

    // A zero-length body is NOT a tombstone on the plain formats: Flink's deserializer hits EOF
    // and fails the job (only the Debezium envelope skips empty messages).
    #[test]
    #[should_panic(expected = "empty message body")]
    fn empty_body_fails_the_plain_avro_decode() {
        let decoder = AvroDecoder::bare(BOUNDARY_WRITER, None, boundary_schema());
        decoder.decode(&bodies(vec![Some(&[])]));
    }

    // A registry writer schema can declare timestamp-micros while the reader (derived from the
    // table under Flink's hard-wired legacy mapping) says timestamp-millis. Avro Java resolves the
    // raw long without unit conversion and Flink then reads it as millis; arrow-avro likewise takes
    // the logical type from the reader and passes the raw long through. Pin that passthrough — a
    // rescale here would silently diverge from Flink.
    #[test]
    fn registry_writer_micros_reads_as_millis_like_flink() {
        let reader = r#"{"type":"record","name":"Row","fields":[
            {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}"#;
        let writer = r#"{"type":"record","name":"Row","fields":[
            {"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"}}]}"#;
        let target = Arc::new(Schema::new(vec![timestamp_ns("ts")]));
        let mut decoder = AvroDecoder::confluent(
            "",
            0,
            Some(AvroSchema::new(reader.to_string())),
            target.clone(),
        );
        decoder.register_writer_schema(7, writer);
        let mut framed = vec![0x00, 0, 0, 0, 7];
        framed.extend(zigzag(5_000)); // 5000 micros on the wire; Flink reads 5000 millis
        let out = decoder.decode(&bodies(vec![Some(&framed)]));
        let ts = out.column(0).as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
        assert_eq!(ts.values(), &[5_000_000_000]);
    }

    // Schema evolution: a reader field the writer lacks materializes its default (the null default
    // every nullable table column carries), and the batch still lands on the boundary schema.
    #[test]
    fn missing_writer_field_takes_the_reader_default() {
        let reader = r#"{"type":"record","name":"Row","fields":[
            {"name":"id","type":"long"},
            {"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"default":null}]}"#;
        let writer = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"long"}]}"#;
        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            timestamp_ns("ts"),
        ]));
        let mut decoder = AvroDecoder::confluent(
            "",
            0,
            Some(AvroSchema::new(reader.to_string())),
            target.clone(),
        );
        decoder.register_writer_schema(3, writer);
        let mut framed = vec![0x00, 0, 0, 0, 3];
        framed.extend(zigzag(42));
        let out = decoder.decode(&bodies(vec![Some(&framed)]));
        assert_eq!(out.schema(), target);
        let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id.values(), &[42]);
        assert!(out.column(1).is_null(0));
    }
}
