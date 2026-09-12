//! Parquet decoding over a host-owned filesystem, following Arroyo's batch reader and Comet's
//! C Data ownership boundary. The host retains discovery, credentials and checkpoint state.
use crate::*;
use arrow::compute::cast;
use bytes::Bytes;
use jni::objects::JObject;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};
use std::io::{Read, Result as IoResult};

struct HostFile {
    vm: jni::JavaVM,
    input: jni::objects::GlobalRef,
    length: u64,
}
impl HostFile {
    fn read(&self, start: u64, out: &mut [u8]) -> IoResult<usize> {
        let size = out
            .len()
            .min(self.length.saturating_sub(start) as usize)
            .min(i32::MAX as usize);
        if size == 0 {
            return Ok(0);
        }
        let mut env = self
            .vm
            .get_env()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        env.with_local_frame(4, |env| -> jni::errors::Result<()> {
            // Java consumes this borrowed buffer synchronously; it never retains the address.
            let buffer = unsafe { env.new_direct_byte_buffer(out.as_mut_ptr(), size)? };
            env.call_method(
                self.input.as_obj(),
                "readFully",
                "(JLjava/nio/ByteBuffer;)V",
                &[
                    jni::objects::JValue::Long(start as i64),
                    jni::objects::JValue::Object(buffer.as_ref()),
                ],
            )?;
            Ok(())
        })
        .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(size)
    }
}
#[derive(Clone)]
struct HostChunks(Arc<HostFile>);
impl Length for HostChunks {
    fn len(&self) -> u64 {
        self.0.length
    }
}
struct HostRead {
    file: Arc<HostFile>,
    offset: u64,
}
impl Read for HostRead {
    fn read(&mut self, out: &mut [u8]) -> IoResult<usize> {
        let count = self.file.read(self.offset, out)?;
        self.offset += count as u64;
        Ok(count)
    }
}
impl ChunkReader for HostChunks {
    type T = HostRead;
    fn get_read(&self, start: u64) -> ParquetResult<HostRead> {
        Ok(HostRead {
            file: self.0.clone(),
            offset: start,
        })
    }
    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        if start
            .checked_add(length as u64)
            .is_none_or(|end| end > self.len())
        {
            return Err(ParquetError::EOF("Parquet read exceeds file length".into()));
        }
        let mut bytes = vec![0; length];
        self.get_read(start)?.read_exact(&mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}
struct ParquetDecoder {
    reader: ParquetRecordBatchReader,
    output: SchemaRef,
    names: Vec<String>,
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_parquet_NativeParquet_createParquetDecoder<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    input: JObject<'local>,
    length: jlong,
    schema_address: jlong,
    names: JObjectArray<'local>,
    batch_size: jint,
) -> jlong {
    bridge::jni_guard(env, |env| {
        let schema = import_schema(schema_address);
        let names: Vec<String> = read_strings(env, &names)
            .into_iter()
            .map(|n| n.expect("Parquet field name"))
            .collect();
        let file = HostChunks(Arc::new(HostFile {
            vm: env.get_java_vm().expect("JVM"),
            input: env.new_global_ref(input).expect("input stream"),
            length: length as u64,
        }));
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("open Parquet file");
        let indices: Vec<usize> = names
            .iter()
            .map(|name| {
                builder
                    .schema()
                    .index_of(name)
                    .unwrap_or_else(|_| panic!("Missing Parquet field {name}"))
            })
            .collect();
        let projection = ProjectionMask::roots(builder.parquet_schema(), indices);
        let reader = builder
            .with_projection(projection)
            .with_batch_size(batch_size as usize)
            .build()
            .expect("Parquet batch reader");
        into_handle(ParquetDecoder {
            reader,
            output: schema,
            names,
        })
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_parquet_NativeParquet_parquetDecoderNext<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) -> jboolean {
    bridge::jni_guard(env, |_env| {
        let decoder = unsafe { &mut *(handle as *mut ParquetDecoder) };
        let Some(batch) = decoder.reader.next() else {
            return 0;
        };
        let batch = batch.expect("decode Parquet batch");
        let columns = decoder
            .names
            .iter()
            .zip(decoder.output.fields())
            .map(|(name, field)| {
                let column = batch.column_by_name(name).expect("projected Parquet field");
                cast(column, field.data_type()).expect("Parquet logical type conversion")
            })
            .collect();
        let output =
            RecordBatch::try_new(decoder.output.clone(), columns).expect("Parquet output schema");
        export_record_batch(output, array, schema);
        1
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_parquet_NativeParquet_closeParquetDecoder<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    bridge::jni_guard(env, |_env| unsafe {
        drop(from_handle::<ParquetDecoder>(handle));
    })
}
