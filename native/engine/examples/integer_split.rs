//! Production compiled Calc, retained JVM computation, and released generated Flink controls.
use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use jni::objects::JValue;
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::CalcProgram;
use streamfusion_bridge::string_cache::{string_cache_array, string_cache_field};

fn program(id: i32, native: bool) -> CalcProgram {
    CalcProgram::new(
        vec![if native { 44 } else { 17 }, 0, 0, 0],
        vec![0, 0, 1, 2],
        vec![3, 0, 0, 0],
        vec![i64::from(id), 0, 11],
        vec![],
        vec![],
    )
}

fn checksum(output: &RecordBatch) -> i64 {
    output
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.map_or(-1, |value| value.len() as i64))
        .sum()
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let corpus = args.get(1).map_or("ascii", String::as_str);
    let width: usize = args.get(2).map_or(264, |s| s.parse().unwrap());
    let cached = args.get(3).is_some_and(|s| s == "true");
    let rows: usize = args.get(4).map_or(1024, |s| s.parse().unwrap());
    let loops: usize = args.get(5).map_or(128, |s| s.parse().unwrap());
    let mixed = args.get(6).is_some_and(|s| s == "true");
    let engine_filter = args.get(7).map_or("all", String::as_str);
    let warmups: usize = args.get(8).map_or(8, |s| s.parse().unwrap());
    let runs: usize = args.get(9).map_or(9, |s| s.parse().unwrap());
    let classpath = format!(
        "-Djava.class.path={}",
        std::env::var("SF_NATIVE_BENCH_CLASSPATH").unwrap()
    );
    let library = format!(
        "-Djava.library.path={}",
        std::env::var("SF_NATIVE_LIB_DIR").unwrap_or_default()
    );
    let options = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&classpath)
        .option(&library)
        .option("-Xms256m")
        .option("-Xmx512m")
        .option("-Dstreamfusion.native.development=true")
        .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(options).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let mut env = vm.attach_current_thread().unwrap();
    let class = "tech/streamfusion/planner/IntegerSplitBenchmarkFunctions";
    let id = env
        .call_static_method(class, "register", "()I", &[])
        .unwrap()
        .i()
        .unwrap();
    let corpus_java = env.new_string(corpus).unwrap();
    env.call_static_method(
        class,
        "prepare",
        "(Ljava/lang/String;IZIZ)V",
        &[
            JValue::Object(&corpus_java),
            JValue::Int(width as i32),
            JValue::Bool(cached.into()),
            JValue::Int(rows as i32),
            JValue::Bool(mixed.into()),
        ],
    )
    .unwrap();
    let unit = match corpus {
        "cjk" => "\u{4e2d}\u{6587}",
        "emoji" => "\u{1f600}",
        _ => "ab",
    };
    let repeats = (width / unit.len()).max(1);
    let strings: Vec<_> = (0..rows)
        .map(|i| {
            let delimiter = [',', '|', '\u{e9}'][i % 3];
            (i % 7 != 0).then(|| {
                format!(
                    "key{}{delimiter}{}{delimiter}tail{}{delimiter}",
                    i % 16,
                    unit.repeat(repeats),
                    i % 16
                )
            })
        })
        .collect();
    let cache = string_cache_array(
        strings
            .iter()
            .map(|s| s.as_ref().map(|s| s.encode_utf16().collect::<Vec<_>>()))
            .collect::<Vec<_>>()
            .iter()
            .map(|units| units.as_deref()),
    );
    let mut fields = vec![
        Field::new("s", DataType::Utf8, true),
        Field::new("delimiter", DataType::Int32, true),
        Field::new("index", DataType::Int32, true),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(strings)),
        Arc::new(Int32Array::from_iter((0..rows).map(|i| {
            (i % 11 != 0).then(|| {
                if mixed && i % 17 == 0 {
                    256
                } else {
                    [44, 124, 233][i % 3]
                }
            })
        }))),
        Arc::new(Int32Array::from_iter((0..rows).map(|i| {
            (i % 13 != 0).then(|| if mixed { i as i32 % 7 - 1 } else { 1 })
        }))),
    ];
    if cached {
        fields.push(string_cache_field(0, cache.data_type().clone()));
        columns.push(cache);
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let mut previous = program(id, false);
    let mut candidate = program(-1, true);
    let reference = previous.run(batch.clone());
    if engine_filter != "baseline" {
        assert_eq!(candidate.run(batch.clone()).column(0), reference.column(0));
    }
    for trial in 0..warmups + runs {
        for engine in 0..3 {
            let name = ["flink", "previous", "native"][engine];
            if engine_filter != "all"
                && engine_filter != name
                && !(engine_filter == "baseline" && engine == 1)
            {
                continue;
            }
            let (nanos, digest) = if engine == 0 {
                let result = env
                    .call_static_method(class, "measure", "(I)[J", &[JValue::Int(loops as i32)])
                    .unwrap()
                    .l()
                    .unwrap();
                let mut values = [0; 2];
                env.get_long_array_region(jni::objects::JLongArray::from(result), 0, &mut values)
                    .unwrap();
                (values[0] as u128, values[1])
            } else {
                let calc = if engine == 1 {
                    &mut previous
                } else {
                    &mut candidate
                };
                let start = Instant::now();
                let mut digest = 0;
                for _ in 0..loops {
                    digest += checksum(&black_box(calc.run(black_box(batch.clone()))));
                }
                (start.elapsed().as_nanos(), black_box(digest))
            };
            assert_eq!(digest, checksum(&reference) * loops as i64);
            println!("integer_split,{corpus},{width},{cached},{rows},{},{mixed},{trial},{},{name},{nanos},{digest}", loops*rows, trial<warmups);
        }
    }
    env.call_static_method(
        "tech/streamfusion/operator/NativeUdf",
        "unregister",
        "(I)V",
        &[JValue::Int(id)],
    )
    .unwrap();
}
