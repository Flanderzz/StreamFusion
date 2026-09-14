//! Benchmark fixtures are copied once into Rust-owned buffers, before either writer is timed.
use super::*;
use arrow::compute::concat_batches;

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_importWriterComparisonBatch(
    env: JNIEnv,
    _class: JClass,
    array: jlong,
    schema: jlong,
) -> jlong {
    bridge::jni_guard(env, |_| {
        let schema = import_schema(schema);
        let batch = import_record_batch_with_schema(array, &schema);
        let split = batch.num_rows() / 2;
        // Two slices force concat to copy, including nested children. A single input may be cloned.
        let owned = concat_batches(
            &schema,
            &[
                batch.slice(0, split),
                batch.slice(split, batch.num_rows() - split),
            ],
        )
        .expect("Copy writer fixture into native buffers");
        into_handle(owned)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_exportWriterComparisonBatch(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
    array: jlong,
) {
    bridge::jni_guard(env, |_| {
        let batch = unsafe { &*(handle as *const RecordBatch) };
        let data = StructArray::from(batch.clone()).to_data();
        unsafe { std::ptr::write(array as *mut FFI_ArrowArray, FFI_ArrowArray::new(&data)) };
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_closeWriterComparisonBatch(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    bridge::jni_guard(env, |_| unsafe {
        drop(from_handle::<RecordBatch>(handle));
    })
}
