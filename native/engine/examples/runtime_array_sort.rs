//! Prepared-input stock/previous/native controls for runtime ARRAY_SORT options.
use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Int32Array, Int64Array, ListArray,
};
use arrow::datatypes::{DataType, Decimal128Type, Field, Int32Type, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use jni::objects::JValue;
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::CalcProgram;

macro_rules! java {
    ($env:ident, $call:expr) => {{
        let result = $call;
        if result.is_err() {
            let _ = $env.exception_describe();
        }
        result.expect("Java benchmark control failed")
    }};
}

fn candidate() -> CalcProgram {
    CalcProgram::new(
        vec![6, 6, 0, 7, 7, 0, 0],
        vec![217, 170, 0, 0, 1, 1, 2],
        vec![3, 3, 0, 0, 0, 0, 0],
        vec![1, 31],
        vec![],
        vec![],
    )
}

fn checksum(batch: &RecordBatch) -> i64 {
    let array = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = array.values();
    match values.data_type() {
        DataType::Int32 => {
            let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
            checksum_values(array, values, |i| i64::from(values.value(i)))
        }
        DataType::Int64 => {
            let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
            checksum_values(array, values, |i| values.value(i))
        }
        _ => {
            let values = values.as_any().downcast_ref::<Decimal128Array>().unwrap();
            checksum_values(array, values, |i| values.value(i) as i64)
        }
    }
}

fn checksum_values(array: &ListArray, values: &dyn Array, value: impl Fn(usize) -> i64) -> i64 {
    let mut sum = 0i64;
    for row in 0..array.len() {
        if array.is_null(row) {
            sum = sum.wrapping_sub(7);
            continue;
        }
        let start = array.value_offsets()[row] as usize;
        let end = array.value_offsets()[row + 1] as usize;
        sum = sum.wrapping_add((end - start) as i64);
        for i in start..end {
            sum = sum.wrapping_add(if values.is_null(i) { -1 } else { value(i) });
        }
    }
    sum
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let element = args.get(1).map_or("int", String::as_str);
    let width: usize = args.get(2).map_or(8, |s| s.parse().unwrap());
    assert!(width < 32);
    let nullable = args.get(3).is_some_and(|s| s == "true");
    let binary = args.get(4).is_some_and(|s| s == "true");
    let rows: usize = args.get(5).map_or(1024, |s| s.parse().unwrap());
    let loops: usize = args.get(6).map_or(32, |s| s.parse().unwrap());
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
        .option("-Xmx1g")
        .option("-Dstreamfusion.native.development=true")
        .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(options).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let mut env = vm.attach_current_thread().unwrap();
    let class = "tech/streamfusion/planner/RuntimeArraySortBenchmarkFunctions";
    let java_type = env.new_string(element).unwrap();
    let id = java!(
        env,
        env.call_static_method(
            class,
            "register",
            "(Ljava/lang/String;)I",
            &[JValue::Object(&java_type)],
        )
    )
    .i()
    .unwrap();
    let schema = java!(
        env,
        env.call_static_method(class, "schema", "()Ljava/lang/String;", &[])
    )
    .l()
    .unwrap();
    let schema: String = env
        .get_string(&jni::objects::JString::from(schema))
        .unwrap()
        .into();
    java!(
        env,
        env.call_static_method(
            class,
            "prepare",
            "(Ljava/lang/String;IZZI)V",
            &[
                JValue::Object(&java_type),
                JValue::Int(width as i32),
                JValue::Bool(nullable.into()),
                JValue::Bool(binary.into()),
                JValue::Int(rows as i32),
            ],
        )
    );
    let data: Vec<Option<Vec<Option<i32>>>> = (0..rows)
        .map(|row| {
            (!nullable || row % 7 != 0).then(|| {
                (0..width)
                    .map(|i| (!nullable || i % 5 != 0).then_some(((row + i * 7) % 17) as i32 - 8))
                    .collect()
            })
        })
        .collect();
    let values: ArrayRef = match element {
        "int" => Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            data.clone(),
        )),
        "bigint" => Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
            data.iter().map(|r| {
                r.as_ref().map(|r| {
                    r.iter()
                        .map(|v| v.map(|v| 9_007_199_254_740_993i64 + i64::from(v)))
                        .collect::<Vec<_>>()
                })
            }),
        )),
        _ => {
            let lists =
                ListArray::from_iter_primitive::<Decimal128Type, _, _>(data.iter().map(|r| {
                    r.as_ref().map(|r| {
                        r.iter()
                            .map(|v| {
                                v.map(|v| {
                                    9_007_199_254_740_993_000_000_001i128
                                        + i128::from(v) * 1_000_000_000
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                }));
            let values = lists
                .values()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .clone()
                .with_precision_and_scale(38, 9)
                .unwrap();
            Arc::new(ListArray::new(
                Arc::new(Field::new_list_field(DataType::Decimal128(38, 9), true)),
                lists.offsets().clone(),
                Arc::new(values),
                lists.nulls().cloned(),
            ))
        }
    };
    let lists = values.as_any().downcast_ref::<ListArray>().unwrap();
    let values: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("element", lists.value_type(), true)),
        lists.offsets().clone(),
        lists.values().clone(),
        lists.nulls().cloned(),
    ));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", values.data_type().clone(), true),
            Field::new("ascending_order", DataType::Boolean, true),
            Field::new("null_first", DataType::Boolean, true),
        ])),
        vec![
            values,
            Arc::new(BooleanArray::from_iter(
                (0..rows).map(|r| (!nullable || r % 11 != 0).then_some(r % 2 == 0)),
            )),
            Arc::new(BooleanArray::from_iter(
                (0..rows).map(|r| (!nullable || r % 13 != 0).then_some(r % 4 < 2)),
            )),
        ],
    )
    .unwrap();
    let mut previous = CalcProgram::new(
        vec![38, 0, 0, 0],
        vec![0, 0, 1, 2],
        vec![3, 0, 0, 0],
        vec![i64::from(id), 0],
        vec![],
        vec![Some(schema)],
    );
    let mut native = candidate();
    let reference = previous.run(batch.clone());
    let candidate = native.run(batch.clone());
    assert_eq!(candidate.schema(), reference.schema());
    assert_eq!(candidate.column(0), reference.column(0));
    let expected = checksum(&reference).wrapping_mul(loops as i64);
    for trial in 0..warmups + runs {
        for engine in (0..3).map(|i| (i + trial) % 3) {
            let (nanos, digest) = if engine == 0 {
                let result = java!(
                    env,
                    env.call_static_method(class, "measure", "(I)[J", &[JValue::Int(loops as i32)])
                )
                .l()
                .unwrap();
                let mut result_array = [0; 2];
                env.get_long_array_region(
                    jni::objects::JLongArray::from(result),
                    0,
                    &mut result_array,
                )
                .unwrap();
                (result_array[0] as u128, result_array[1])
            } else {
                let calc = if engine == 1 {
                    &mut previous
                } else {
                    &mut native
                };
                let start = Instant::now();
                let mut digest = 0i64;
                for _ in 0..loops {
                    digest = digest
                        .wrapping_add(checksum(&black_box(calc.run(black_box(batch.clone())))));
                }
                (start.elapsed().as_nanos(), black_box(digest))
            };
            assert_eq!(digest, expected);
            println!("runtime_array_sort,{element},{width},{nullable},{binary},{rows},{},{trial},{},{},{nanos},{digest}",
                loops*rows,trial<warmups,["flink","previous","native"][engine]);
        }
    }
    java!(env, env.call_static_method(class, "close", "()V", &[]));
}
