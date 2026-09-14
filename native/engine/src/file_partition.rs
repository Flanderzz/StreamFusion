use crate::*;
use streamfusion_bridge::partition::split_by_partition_columns;

/// Splits an Arrow batch the JVM exported by its partition-key columns and returns a handle to the
/// resulting groups, pulled one at a time with `nextPartitionSlice` and released with
/// `closePartitionSplit`.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_splitByPartitionColumns<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    partition_columns: JIntArray<'local>,
) -> jlong {
    crate::bridge::jni_guard(env, move |env| {
        let batch = import_record_batch(in_array_address, in_schema_address);
        let partition_columns = read_columns(&env, &partition_columns);
        let slices = split_by_partition_columns(&batch, &partition_columns);
        into_handle(PartitionSplit {
            slices: slices.into_iter().rev().collect(),
        })
    })
}

/// Per-partition groups awaiting export, in reverse so the JVM pulls them in first-seen order.
pub(crate) struct PartitionSplit {
    slices: Vec<RecordBatch>,
}

/// Exports the next partition group into the consumer-allocated C structs, returning false once
/// every group has been pulled.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_nextPartitionSlice<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) -> jboolean {
    crate::bridge::jni_guard(env, move |_env| {
        let split = unsafe { &mut *(handle as *mut PartitionSplit) };
        match split.slices.pop() {
            Some(slice) => {
                export_record_batch(slice, out_array_address, out_schema_address);
                1
            }
            None => 0,
        }
    })
}

/// Releases a partition split handle, dropping any groups the JVM did not pull.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closePartitionSplit<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<PartitionSplit>(handle));
    })
}
