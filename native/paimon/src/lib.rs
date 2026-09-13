use jni::objects::{GlobalRef, JObject, JValue};
use streamfusion_bridge::prelude::*;
use streamfusion_bridge::{self as bridge, *};
streamfusion_bridge::link_allocator!();
mod loser_tree;
mod merge;

struct SnapshotMerger {
    merger: merge::Merger,
    provider: GlobalRef,
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_paimon_NativePaimon_nativeBuildVersion(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    bridge::version_probe(env)
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_paimon_NativePaimon_liveNativeHandles(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    bridge::live_handles_probe(env)
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_paimon_NativePaimon_createSnapshotMerger<'a>(
    env: JNIEnv<'a>,
    _class: JClass<'a>,
    provider: JObject<'a>,
    input_schema: jlong,
    output_schema: jlong,
    keys: jint,
    runs: jint,
    rows: jint,
    budget: jlong,
) -> jlong {
    bridge::jni_guard(env, |env| {
        let input = import_schema(input_schema);
        let output = import_schema(output_schema);
        assert!(keys >= 0 && runs > 0 && rows > 0 && budget > 0);
        into_handle(SnapshotMerger {
            merger: merge::Merger::new(
                input,
                output,
                keys as usize,
                runs as usize,
                rows as usize,
                budget as usize,
            )
            .expect("snapshot merger"),
            provider: env
                .new_global_ref(provider)
                .expect("snapshot input provider"),
        })
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_paimon_NativePaimon_snapshotMergerNext(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
    array: jlong,
    schema: jlong,
) -> jboolean {
    bridge::jni_guard(env, |env| {
        let state = unsafe { &mut *(handle as *mut SnapshotMerger) };
        let result = state
            .merger
            .next(&mut |run| {
                let mut input = FFI_ArrowArray::empty();
                let mut input_schema = FFI_ArrowSchema::empty();
                // Comet ownership pattern: the caller owns the structs; imported buffers retain
                // producer release callbacks across the two native libraries.
                let available = env
                    .with_local_frame(4, |env| {
                        env.call_method(
                            state.provider.as_obj(),
                            "nextRunBatch",
                            "(IJJ)Z",
                            &[
                                JValue::Int(run as i32),
                                JValue::Long((&mut input as *mut FFI_ArrowArray) as i64),
                                JValue::Long((&mut input_schema as *mut FFI_ArrowSchema) as i64),
                            ],
                        )
                        .and_then(|v| v.z())
                    })
                    .map_err(|e| e.to_string())?;
                if !available {
                    return Ok(None);
                }
                Ok(Some(import_record_batch(
                    (&mut input as *mut FFI_ArrowArray) as i64,
                    (&mut input_schema as *mut FFI_ArrowSchema) as i64,
                )))
            })
            .expect("Paimon snapshot merge");
        if let Some(batch) = result {
            export_record_batch(batch, array, schema);
            1
        } else {
            0
        }
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_paimon_NativePaimon_snapshotMergerPeakBytes(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jlong {
    bridge::jni_guard(env, |_| unsafe {
        (*(handle as *mut SnapshotMerger)).merger.peak_bytes as i64
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_paimon_NativePaimon_closeSnapshotMerger(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    bridge::jni_guard(env, |_| unsafe {
        drop(from_handle::<SnapshotMerger>(handle));
    })
}
