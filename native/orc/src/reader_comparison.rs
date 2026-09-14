//! Compare production decoding with Java at the engine's Arrow Rust consumption boundary.
use super::*;
use arrow::util::display::array_value_to_string;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Default)]
struct Stats {
    rows: i64,
    checksum: i64,
    peak_batch_bytes: i64,
    digest: u64,
}
impl Stats {
    fn consume(&mut self, batch: RecordBatch, verify: bool) {
        self.rows += batch.num_rows() as i64;
        self.peak_batch_bytes = self
            .peak_batch_bytes
            .max(batch.get_array_memory_size() as i64);
        let id = batch.column(0);
        if let Some(ids) = id.as_any().downcast_ref::<Int64Array>() {
            self.checksum += ids.iter().flatten().sum::<i64>();
        } else if let Some(ids) = id.as_any().downcast_ref::<Int32Array>() {
            self.checksum += ids.iter().flatten().map(i64::from).sum::<i64>();
        }
        if verify {
            for row in 0..batch.num_rows() {
                let mut hash = DefaultHasher::new();
                for array in batch.columns() {
                    array.is_null(row).hash(&mut hash);
                    if !array.is_null(row) {
                        array_value_to_string(array, row).unwrap().hash(&mut hash);
                    }
                }
                self.digest = self.digest.wrapping_add(hash.finish());
            }
        }
        std::hint::black_box(batch);
    }
    fn export(&self, env: &mut JNIEnv) -> jni::sys::jlongArray {
        let result = env.new_long_array(4).unwrap();
        env.set_long_array_region(
            &result,
            0,
            &[
                self.rows,
                self.checksum,
                self.peak_batch_bytes,
                self.digest as i64,
            ],
        )
        .unwrap();
        result.into_raw()
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_compareReaders<'a>(
    env: JNIEnv<'a>,
    _class: JClass<'a>,
    input: JObject<'a>,
    length: jlong,
    schema: jlong,
    names: JObjectArray<'a>,
    batch_size: jint,
    verify: jboolean,
) -> jni::sys::jlongArray {
    bridge::jni_guard(env, |env| {
        let schema = import_schema(schema);
        let names = read_strings(env, &names)
            .into_iter()
            .map(|n| n.expect("ORC name"))
            .collect();
        let mut reader = reader::Decoder::open(
            HostIo::new(env, input),
            length.try_into().unwrap(),
            schema,
            names,
            batch_size.try_into().unwrap(),
            "",
        );
        let mut stats = Stats::default();
        while let Some(batch) = reader.next() {
            stats.consume(batch, verify != 0);
        }
        stats.export(env)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_consumeComparisonBatch(
    env: JNIEnv,
    _class: JClass,
    array: jlong,
    schema: jlong,
    verify: jboolean,
) -> jni::sys::jlongArray {
    bridge::jni_guard(env, |env| {
        let schema = import_schema(schema);
        let mut stats = Stats::default();
        stats.consume(import_record_batch_with_schema(array, &schema), verify != 0);
        stats.export(env)
    })
}
