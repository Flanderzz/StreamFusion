//! Opt-in benchmark: all decoders end at the Arrow version consumed by our operators.
use super::*;
use arrow::util::display::array_value_to_string;
use orc_rust::{projection::ProjectionMask, reader::ChunkReader, ArrowReader, ArrowReaderBuilder};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{self, Read};
use std::mem::ManuallyDrop;
use std::rc::Rc;

unsafe extern "C" {
    fn sf_arrow_orc_error() -> *const c_char;
    fn sf_arrow_orc_open(
        read: extern "C" fn(*mut c_void, u64, u64, *mut c_void) -> c_int,
        context: *mut c_void,
        length: i64,
        batch_size: i64,
        names: *const *const c_char,
        count: usize,
        schema: *mut FFI_ArrowSchema,
    ) -> *mut c_void;
    fn sf_arrow_orc_next(reader: *mut c_void, array: *mut FFI_ArrowArray) -> c_int;
    fn sf_arrow_orc_close(reader: *mut c_void);
}

#[derive(Clone)]
struct HostChunks {
    host: Rc<HostIo>,
    length: u64,
}
struct HostRead {
    chunks: HostChunks,
    offset: u64,
}
impl Read for HostRead {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let length = (buffer.len() as u64).min(self.chunks.length.saturating_sub(self.offset));
        if read_host(
            Rc::as_ptr(&self.chunks.host) as *mut c_void,
            self.offset,
            length,
            buffer.as_mut_ptr().cast(),
        ) != 0
        {
            return Err(io::Error::other("Host ORC input failed"));
        }
        self.offset += length;
        Ok(length as usize)
    }
}
impl ChunkReader for HostChunks {
    type T = HostRead;
    fn len(&self) -> u64 {
        self.length
    }
    fn get_read(&self, offset: u64) -> io::Result<Self::T> {
        Ok(HostRead {
            chunks: self.clone(),
            offset,
        })
    }
}

struct CppReader {
    native: NonNull<c_void>,
    arrow_cpp: bool,
    schema: SchemaRef,
    _host: Rc<HostIo>,
}
impl Drop for CppReader {
    fn drop(&mut self) {
        unsafe {
            if self.arrow_cpp {
                sf_arrow_orc_close(self.native.as_ptr());
            } else {
                sf_orc_reader_free(self.native.as_ptr());
            }
        }
    }
}
enum Reader {
    Cpp(CppReader),
    Rust(ArrowReader<HostChunks>, SchemaRef),
}

impl Reader {
    fn open(
        backend: jint,
        host: Rc<HostIo>,
        length: u64,
        schema: SchemaRef,
        names: &[CString],
        batch_size: usize,
    ) -> Self {
        assert!(batch_size > 0);
        let context = Rc::as_ptr(&host) as *mut c_void;
        match backend {
            0 => {
                let ffi = FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
                let native = unsafe {
                    sf_orc_reader_new(
                        &ffi,
                        pointers(names).as_ptr(),
                        names.len(),
                        length,
                        batch_size as u64,
                        std::ptr::null(),
                        read_host,
                        context,
                    )
                };
                Self::Cpp(CppReader {
                    native: NonNull::new(native).unwrap_or_else(|| panic!("{}", error())),
                    arrow_cpp: false,
                    schema,
                    _host: host,
                })
            }
            1 => {
                let mut ffi = FFI_ArrowSchema::empty();
                let native = unsafe {
                    sf_arrow_orc_open(
                        read_host,
                        context,
                        length as i64,
                        batch_size as i64,
                        pointers(names).as_ptr(),
                        names.len(),
                        &mut ffi,
                    )
                };
                let native = NonNull::new(native).unwrap_or_else(|| {
                    panic!(
                        "Arrow C++: {}",
                        unsafe { CStr::from_ptr(sf_arrow_orc_error()) }.to_string_lossy()
                    )
                });
                // Establish the C++ owner before importing the schema so a failed import closes it.
                let mut reader = CppReader {
                    native,
                    arrow_cpp: true,
                    schema,
                    _host: host,
                };
                reader.schema = Arc::new(Schema::try_from(&ffi).expect("Arrow C++ schema"));
                Self::Cpp(reader)
            }
            2 => {
                let builder = ArrowReaderBuilder::try_new(HostChunks { host, length })
                    .expect("orc-rust open");
                let names: Vec<_> = names.iter().map(|n| n.to_str().unwrap()).collect();
                let projection =
                    ProjectionMask::named_roots(builder.file_metadata().root_data_type(), &names);
                let builder = builder
                    .with_projection(projection)
                    .with_batch_size(batch_size);
                let ffi59 =
                    arrow59::ffi::FFI_ArrowSchema::try_from(builder.schema().as_ref()).unwrap();
                // Both versions implement the standard repr(C) Arrow C Data ABI. Borrow the schema;
                // its release callback stays owned by Arrow 59 and runs after Arrow 58 copies metadata.
                let ffi58 = unsafe { &*(&ffi59 as *const _ as *const FFI_ArrowSchema) };
                let natural = Arc::new(Schema::try_from(ffi58).expect("orc-rust schema"));
                Self::Rust(builder.build(), natural)
            }
            _ => panic!("Unknown ORC comparison backend {backend}"),
        }
    }

    fn next(&mut self) -> Option<RecordBatch> {
        let (array, schema) = match self {
            Self::Cpp(reader) => {
                let mut array = FFI_ArrowArray::empty();
                let status = unsafe {
                    if reader.arrow_cpp {
                        sf_arrow_orc_next(reader.native.as_ptr(), &mut array)
                    } else {
                        sf_orc_reader_next(reader.native.as_ptr(), &mut array)
                    }
                };
                if reader.arrow_cpp {
                    assert!(status >= 0, "Arrow C++: {}", unsafe {
                        CStr::from_ptr(sf_arrow_orc_error()).to_string_lossy()
                    });
                } else {
                    check(status);
                }
                if status == 0 {
                    return None;
                }
                (array, &reader.schema)
            }
            Self::Rust(reader, schema) => {
                use arrow59::array::Array as _;
                let batch = reader.next()?.expect("orc-rust decode");
                let data = arrow59::array::StructArray::from(batch).to_data();
                let ffi59 = ManuallyDrop::new(arrow59::ffi::FFI_ArrowArray::new(&data));
                // Transfer C Data ownership, without copying buffers. Arrow 58 invokes the Arrow 59
                // release callback only when its last imported buffer owner is dropped.
                let ffi58 = unsafe { std::ptr::read(&*ffi59 as *const _ as *const FFI_ArrowArray) };
                (ffi58, &*schema)
            }
        };
        let data =
            unsafe { from_ffi_and_data_type(array, DataType::Struct(schema.fields().clone())) }
                .expect("Import comparison batch into arrow-rs");
        Some(RecordBatch::from(StructArray::from(data)))
    }
}

fn project(batch: RecordBatch, schema: &SchemaRef, names: &[CString]) -> RecordBatch {
    let columns = names
        .iter()
        .zip(schema.fields())
        .map(|(name, field)| {
            let array = batch
                .column_by_name(name.to_str().unwrap())
                .expect("Projected ORC field");
            arrow::compute::cast(array, field.data_type())
                .expect("Cast ORC field to operator schema")
        })
        .collect();
    RecordBatch::try_new(schema.clone(), columns).expect("Operator RecordBatch")
}

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
    backend: jint,
    input: JObject<'a>,
    length: jlong,
    schema: jlong,
    names: JObjectArray<'a>,
    batch_size: jint,
    verify: jboolean,
) -> jni::sys::jlongArray {
    bridge::jni_guard(env, |env| {
        let schema = import_schema(schema);
        let names = strings(env, &names);
        assert_eq!(names.len(), schema.fields().len());
        let host = Rc::new(HostIo::new(env, input, None));
        let mut reader = Reader::open(
            backend,
            host,
            length.try_into().unwrap(),
            schema.clone(),
            &names,
            batch_size.try_into().unwrap(),
        );
        let mut stats = Stats::default();
        while let Some(batch) = reader.next() {
            let batch = if backend == 0 {
                batch
            } else {
                project(batch, &schema, &names)
            };
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
