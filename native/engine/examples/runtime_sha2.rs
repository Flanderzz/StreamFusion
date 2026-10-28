//! Production runtime-width hashing with released generated Flink and previous native controls.
use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use jni::objects::JValue;
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::CalcProgram;

fn program(id: i32, native: bool) -> CalcProgram {
    CalcProgram::new(
        vec![if native { 44 } else { 17 }, 0, 0],
        vec![0, 0, 1],
        vec![2, 0, 0],
        vec![i64::from(id), 0, 12],
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
    let engine_filter = args.get(6).map_or("all", String::as_str);
    let warmups: usize = args.get(7).map_or(8, |s| s.parse().unwrap());
    let runs: usize = args.get(8).map_or(9, |s| s.parse().unwrap());
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
    let class = "tech/streamfusion/planner/RuntimeSha2BenchmarkFunctions";
    let id = env
        .call_static_method(class, "register", "()I", &[])
        .unwrap()
        .i()
        .unwrap();
    let corpus_java = env.new_string(corpus).unwrap();
    env.call_static_method(
        class,
        "prepare",
        "(Ljava/lang/String;IZI)V",
        &[
            JValue::Object(&corpus_java),
            JValue::Int(width as i32),
            JValue::Bool(cached.into()),
            JValue::Int(rows as i32),
        ],
    )
    .unwrap();
    let unit = match corpus {
        "cjk" => "\u{4e2d}\u{6587}",
        "emoji" => "\u{1f600}",
        _ => "ab",
    };
    let repeats = (width / unit.len()).max(1);
    let strings = StringArray::from_iter(
        (0..rows).map(|i| (i % 7 != 0).then(|| format!("key{}{}", i % 16, unit.repeat(repeats)))),
    );
    let bits = Int32Array::from_iter(
        (0..rows).map(|i| (i % 11 != 0).then(|| [224, 256, 384, 512][i % 4])),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("bits", DataType::Int32, true),
        ])),
        vec![Arc::new(strings) as ArrayRef, Arc::new(bits)],
    )
    .unwrap();
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
            println!("runtime_sha2,{corpus},{width},{cached},{rows},{},{trial},{},{name},{nanos},{digest}",
                loops*rows, trial<warmups);
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
