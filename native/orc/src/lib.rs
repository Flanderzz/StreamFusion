//! Apache ORC C++ vector batches behind the same host-owned I/O and C Data boundary as Parquet.
use jni::objects::{GlobalRef, JObject, JValue};
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr::NonNull;
use streamfusion_bridge::prelude::*;
use streamfusion_bridge::{self as bridge, *};
streamfusion_bridge::link_allocator!();

#[cfg(feature = "reader-comparison")]
mod reader_comparison;

#[cfg(feature = "writer-comparison")]
mod writer_comparison;

unsafe extern "C" {
    fn sf_orc_error() -> *const c_char;
    fn sf_orc_writer_new(
        schema: *const FFI_ArrowSchema,
        description: *const c_char,
        keys: *const *const c_char,
        values: *const *const c_char,
        count: usize,
        write: extern "C" fn(*mut c_void, *const c_void, usize) -> c_int,
        context: *mut c_void,
    ) -> *mut c_void;
    fn sf_orc_writer_write(writer: *mut c_void, array: *const FFI_ArrowArray) -> c_int;
    fn sf_orc_writer_finish(writer: *mut c_void) -> c_int;
    fn sf_orc_writer_size(writer: *mut c_void) -> u64;
    fn sf_orc_writer_free(writer: *mut c_void);
    fn sf_orc_reader_new(
        schema: *const FFI_ArrowSchema,
        names: *const *const c_char,
        count: usize,
        length: u64,
        batch_size: u64,
        instant_zone: *const c_char,
        read: extern "C" fn(*mut c_void, u64, u64, *mut c_void) -> c_int,
        context: *mut c_void,
    ) -> *mut c_void;
    fn sf_orc_reader_next(reader: *mut c_void, array: *mut FFI_ArrowArray) -> c_int;
    fn sf_orc_reader_memory(reader: *mut c_void) -> u64;
    fn sf_orc_reader_free(reader: *mut c_void);
}
fn error() -> String {
    unsafe {
        CStr::from_ptr(sf_orc_error())
            .to_string_lossy()
            .into_owned()
    }
}
fn check(status: c_int) {
    assert!(status >= 0, "ORC: {}", error());
}
fn strings(env: &mut JNIEnv, values: &JObjectArray) -> Vec<CString> {
    read_strings(env, values)
        .into_iter()
        .map(|s| CString::new(s.expect("null ORC option")).expect("ORC option contains NUL"))
        .collect()
}
fn pointers(strings: &[CString]) -> Vec<*const c_char> {
    strings.iter().map(|s| s.as_ptr()).collect()
}
struct HostIo {
    vm: jni::JavaVM,
    stream: GlobalRef,
    chunk: Option<GlobalRef>,
}
impl HostIo {
    fn new(env: &mut JNIEnv, stream: JObject, chunk: Option<JByteArray>) -> Self {
        Self {
            vm: env.get_java_vm().expect("JVM"),
            stream: env.new_global_ref(stream).expect("ORC stream"),
            chunk: chunk.map(|c| env.new_global_ref(c).expect("ORC output chunk")),
        }
    }
}
// C++ catches its own exceptions. Callbacks likewise never unwind Rust across a C++ frame;
// any Java I/O exception remains pending for the enclosing JNI guard to return to the host.
extern "C" fn read_host(
    context: *mut c_void,
    offset: u64,
    length: u64,
    bytes: *mut c_void,
) -> c_int {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || -> jni::errors::Result<()> {
            let host = unsafe { &*(context as *const HostIo) };
            let mut env = host.vm.get_env()?;
            let mut done = 0;
            while done < length {
                let size = (length - done).min(i32::MAX as u64) as usize;
                env.with_local_frame(4, |env| -> jni::errors::Result<()> {
                    let buffer = unsafe {
                        env.new_direct_byte_buffer(bytes.cast::<u8>().add(done as usize), size)?
                    };
                    env.call_method(
                        host.stream.as_obj(),
                        "readFully",
                        "(JLjava/nio/ByteBuffer;)V",
                        &[
                            JValue::Long((offset + done) as i64),
                            JValue::Object(buffer.as_ref()),
                        ],
                    )?;
                    Ok(())
                })?;
                done += size as u64;
            }
            Ok(())
        },
    ))
    .map_or(-1, |result| if result.is_ok() { 0 } else { -1 })
}
extern "C" fn write_host(context: *mut c_void, bytes: *const c_void, length: usize) -> c_int {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || -> jni::errors::Result<()> {
            let host = unsafe { &*(context as *const HostIo) };
            let mut env = host.vm.get_env()?;
            let array: &JByteArray = host.chunk.as_ref().expect("output chunk").as_obj().into();
            let capacity = env.get_array_length(array)? as usize;
            assert!(capacity > 0, "empty ORC output chunk");
            let input = unsafe { std::slice::from_raw_parts(bytes.cast::<i8>(), length) };
            for chunk in input.chunks(capacity) {
                env.set_byte_array_region(array, 0, chunk)?;
                env.call_method(
                    host.stream.as_obj(),
                    "write",
                    "([BII)V",
                    &[
                        JValue::Object(array.as_ref()),
                        JValue::Int(0),
                        JValue::Int(chunk.len() as i32),
                    ],
                )?;
            }
            Ok(())
        },
    ))
    .map_or(-1, |result| if result.is_ok() { 0 } else { -1 })
}
struct Encoder {
    native: NonNull<c_void>,
    _host: Box<HostIo>,
    input: SchemaRef,
    projection: Vec<usize>,
}
impl Drop for Encoder {
    fn drop(&mut self) {
        unsafe {
            sf_orc_writer_free(self.native.as_ptr());
        }
    }
}
struct Decoder {
    native: NonNull<c_void>,
    _host: Box<HostIo>,
    schema: SchemaRef,
}
impl Drop for Decoder {
    fn drop(&mut self) {
        unsafe {
            sf_orc_reader_free(self.native.as_ptr());
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_nativeBuildVersion(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    bridge::version_probe(env)
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_liveNativeHandles(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    bridge::live_handles_probe(env)
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_createOrcEncoder<'a>(
    env: JNIEnv<'a>,
    _class: JClass<'a>,
    schema_address: jlong,
    description: JString<'a>,
    partitions: JIntArray<'a>,
    keys: JObjectArray<'a>,
    values: JObjectArray<'a>,
    output: JObject<'a>,
    chunk: JByteArray<'a>,
) -> jlong {
    bridge::jni_guard(env, |env| {
        let input = import_schema(schema_address);
        let partitions = read_columns(env, &partitions);
        let projection: Vec<_> = (0..input.fields().len())
            .filter(|i| !partitions.contains(i))
            .collect();
        let schema = input.project(&projection).expect("ORC projection");
        let ffi = FFI_ArrowSchema::try_from(&schema).expect("ORC schema export");
        let description = CString::new(
            env.get_string(&description)
                .expect("ORC type")
                .to_str()
                .expect("ORC UTF8 type"),
        )
        .expect("ORC type contains NUL");
        let keys = strings(env, &keys);
        let values = strings(env, &values);
        assert_eq!(keys.len(), values.len());
        let mut host = Box::new(HostIo::new(env, output, Some(chunk)));
        let native = unsafe {
            sf_orc_writer_new(
                &ffi,
                description.as_ptr(),
                pointers(&keys).as_ptr(),
                pointers(&values).as_ptr(),
                keys.len(),
                write_host,
                (&mut *host as *mut HostIo).cast(),
            )
        };
        let native = NonNull::new(native).unwrap_or_else(|| panic!("Open ORC writer: {}", error()));
        into_handle(Encoder {
            native,
            _host: host,
            input,
            projection,
        })
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_orcEncoderWrite<'a>(
    env: JNIEnv<'a>,
    _class: JClass<'a>,
    handle: jlong,
    array: jlong,
    selected: JIntArray<'a>,
    offset: jint,
    count: jint,
) {
    bridge::jni_guard(env, |env| {
        let writer = unsafe { &mut *(handle as *mut Encoder) };
        assert_eq!(
            record_batch_column_count(array),
            writer.input.fields().len()
        );
        let batch = import_record_batch_with_schema(array, &writer.input);
        let selected = read_columns(env, &selected);
        let batch = batch.project(&writer.projection).expect("ORC projection");
        let batch = if selected.is_empty() {
            let offset = usize::try_from(offset).expect("negative ORC row offset");
            let count = if count < 0 {
                batch.num_rows().checked_sub(offset).expect("ORC offset")
            } else {
                count as usize
            };
            batch.slice(offset, count)
        } else {
            assert_eq!(offset, 0);
            assert!(count < 0 || count as usize == selected.len());
            let indices = UInt32Array::from(
                selected
                    .iter()
                    .map(|i| u32::try_from(*i).expect("ORC selected row"))
                    .collect::<Vec<_>>(),
            );
            RecordBatch::try_new(
                batch.schema(),
                batch
                    .columns()
                    .iter()
                    .map(|c| take(c.as_ref(), &indices, None).expect("ORC selected rows"))
                    .collect(),
            )
            .expect("ORC selected batch")
        };
        let data = StructArray::from(batch).to_data();
        let array = FFI_ArrowArray::new(&data);
        check(unsafe { sf_orc_writer_write(writer.native.as_ptr(), &array) });
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_orcEncoderEstimatedBytes(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jlong {
    bridge::jni_guard(env, |_| {
        let writer = unsafe { &*(handle as *const Encoder) };
        unsafe { sf_orc_writer_size(writer.native.as_ptr()).min(i64::MAX as u64) as i64 }
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_orcEncoderFinish(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    bridge::jni_guard(env, |_| {
        let writer = unsafe { &mut *(handle as *mut Encoder) };
        check(unsafe { sf_orc_writer_finish(writer.native.as_ptr()) });
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_closeOrcEncoder(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    bridge::jni_guard(env, |_| unsafe {
        drop(from_handle::<Encoder>(handle));
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_createOrcDecoder<'a>(
    env: JNIEnv<'a>,
    _class: JClass<'a>,
    input: JObject<'a>,
    length: jlong,
    schema: jlong,
    names: JObjectArray<'a>,
    batch_size: jint,
    instant_zone: JString<'a>,
) -> jlong {
    bridge::jni_guard(env, |env| {
        assert!(length >= 0 && batch_size > 0);
        let schema = import_schema(schema);
        let ffi = FFI_ArrowSchema::try_from(schema.as_ref()).expect("ORC schema export");
        let names = strings(env, &names);
        assert_eq!(names.len(), schema.fields().len());
        let instant_zone = CString::new(
            env.get_string(&instant_zone)
                .expect("ORC instant timezone")
                .to_str()
                .unwrap(),
        )
        .unwrap();
        let mut host = Box::new(HostIo::new(env, input, None));
        let native = unsafe {
            sf_orc_reader_new(
                &ffi,
                pointers(&names).as_ptr(),
                names.len(),
                length as u64,
                batch_size as u64,
                instant_zone.as_ptr(),
                read_host,
                (&mut *host as *mut HostIo).cast(),
            )
        };
        let native = NonNull::new(native).unwrap_or_else(|| panic!("Open ORC reader: {}", error()));
        into_handle(Decoder {
            native,
            _host: host,
            schema,
        })
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_orcDecoderNext(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
    array: jlong,
    schema: jlong,
) -> jboolean {
    bridge::jni_guard(env, |_| {
        let decoder = unsafe { &mut *(handle as *mut Decoder) };
        let mut output = FFI_ArrowArray::empty();
        let status = unsafe { sf_orc_reader_next(decoder.native.as_ptr(), &mut output) };
        check(status);
        if status == 0 {
            return 0;
        }
        let data = unsafe {
            from_ffi_and_data_type(output, DataType::Struct(decoder.schema.fields().clone()))
        }
        .expect("Import ORC Arrow batch");
        let batch = RecordBatch::from(StructArray::from(data));
        export_record_batch(batch, array, schema);
        1
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_orcDecoderMaxStripeBytes(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jlong {
    bridge::jni_guard(env, |_| {
        let decoder = unsafe { &*(handle as *const Decoder) };
        unsafe { sf_orc_reader_memory(decoder.native.as_ptr()).min(i64::MAX as u64) as i64 }
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_closeOrcDecoder(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    bridge::jni_guard(env, |_| unsafe {
        drop(from_handle::<Decoder>(handle));
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    extern "C" fn output(ctx: *mut c_void, bytes: *const c_void, len: usize) -> c_int {
        unsafe {
            (&mut *ctx.cast::<Vec<u8>>())
                .extend_from_slice(std::slice::from_raw_parts(bytes.cast(), len));
        }
        0
    }
    extern "C" fn input(ctx: *mut c_void, offset: u64, len: u64, bytes: *mut c_void) -> c_int {
        unsafe {
            let source = &*ctx.cast::<Vec<u8>>();
            if offset.saturating_add(len) > source.len() as u64 {
                return -1;
            }
            std::ptr::copy_nonoverlapping(
                source.as_ptr().add(offset as usize),
                bytes.cast(),
                len as usize,
            );
        }
        0
    }
    #[test]
    fn sliced_nested_batches_and_projection_roundtrip_all_codecs() {
        use arrow::array::{Date32Array, Float64Array, Int32Builder, ListBuilder};
        let mut list = ListBuilder::new(Int32Builder::new());
        for i in 0..7 {
            list.values().append_value(i);
            list.values().append_null();
            list.append(i != 3);
        }
        let nested = StructArray::new(
            vec![Arc::new(Field::new("x", DataType::Int32, true))].into(),
            vec![Arc::new(Int32Array::from(vec![
                Some(9),
                None,
                Some(1),
                None,
                Some(-1),
                Some(2),
                None,
            ]))],
            Some(arrow::buffer::NullBuffer::from(vec![
                true, false, true, false, true, true, true,
            ])),
        );
        let decimal = 10_i128.pow(38) - 1;
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6])) as ArrayRef,
            ),
            (
                "text",
                Arc::new(StringArray::from(vec![
                    Some("skip"),
                    Some("é😀"),
                    None,
                    Some(""),
                    Some("prefix "),
                    Some("尾"),
                    Some("skip"),
                ])),
            ),
            (
                "decimal",
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(0),
                        Some(decimal),
                        None,
                        Some(-decimal),
                        Some(-1),
                        Some(1),
                        Some(0),
                    ])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
                ),
            ),
            (
                "ts",
                Arc::new(TimestampMicrosecondArray::from(vec![
                    0, -2000001, -1000001, -1000000, 0, 1, 0,
                ])),
            ),
            (
                "date",
                Arc::new(Date32Array::from(vec![0, -20000, -1, 0, 1, 20000, 0])),
            ),
            (
                "double",
                Arc::new(Float64Array::from(vec![
                    Some(0.),
                    Some(-0.),
                    None,
                    Some(f64::NEG_INFINITY),
                    Some(1.25),
                    Some(f64::INFINITY),
                    Some(0.),
                ])),
            ),
            ("list", Arc::new(list.finish())),
            ("nested", Arc::new(nested)),
        ])
        .unwrap()
        .slice(1, 5);
        let description = CString::new("struct<id:int,text:string,decimal:decimal(38,2),ts:timestamp,date:date,double:double,list:array<int>,nested:struct<x:int>>").unwrap();
        let schema = FFI_ArrowSchema::try_from(batch.schema().as_ref()).unwrap();
        let array = FFI_ArrowArray::new(&StructArray::from(batch.clone()).to_data());
        for codec in ["NONE", "ZLIB", "SNAPPY", "LZ4", "ZSTD"] {
            let mut bytes = Vec::<u8>::new();
            let keys = [
                CString::new("compression").unwrap(),
                CString::new("compress.size").unwrap(),
            ];
            let values = [CString::new(codec).unwrap(), CString::new("4096").unwrap()];
            let writer = unsafe {
                sf_orc_writer_new(
                    &schema,
                    description.as_ptr(),
                    pointers(&keys).as_ptr(),
                    pointers(&values).as_ptr(),
                    keys.len(),
                    output,
                    (&mut bytes as *mut Vec<u8>).cast(),
                )
            };
            assert!(!writer.is_null(), "{}", error());
            check(unsafe { sf_orc_writer_write(writer, &array) });
            check(unsafe { sf_orc_writer_finish(writer) });
            unsafe {
                sf_orc_writer_free(writer);
            }
            let projection: Vec<_> = (0..batch.num_columns()).rev().collect();
            let expected = batch.project(&projection).unwrap();
            let out_schema = FFI_ArrowSchema::try_from(expected.schema().as_ref()).unwrap();
            let names: Vec<_> = expected
                .schema()
                .fields()
                .iter()
                .map(|f| CString::new(f.name().as_str()).unwrap())
                .collect();
            let reader = unsafe {
                sf_orc_reader_new(
                    &out_schema,
                    pointers(&names).as_ptr(),
                    names.len(),
                    bytes.len() as u64,
                    2,
                    std::ptr::null(),
                    input,
                    (&mut bytes as *mut Vec<u8>).cast(),
                )
            };
            assert!(!reader.is_null(), "{}", error());
            let mut batches = Vec::new();
            loop {
                let mut array = FFI_ArrowArray::empty();
                let status = unsafe { sf_orc_reader_next(reader, &mut array) };
                check(status);
                if status == 0 {
                    break;
                }
                let data = unsafe {
                    from_ffi_and_data_type(
                        array,
                        DataType::Struct(expected.schema().fields().clone()),
                    )
                }
                .unwrap();
                batches.push(RecordBatch::from(StructArray::from(data)));
            }
            unsafe {
                sf_orc_reader_free(reader);
            }
            assert_eq!(
                concat_batches(&expected.schema(), &batches).unwrap(),
                expected,
                "{codec}"
            );
        }
    }
}
