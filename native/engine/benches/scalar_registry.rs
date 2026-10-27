//! Every registered Flink scalar kernel, with sliced nullable arrays and literal adaptation.
use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
    Int64Builder, ListBuilder, MapBuilder, StringArray,
};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::common::{config::ConfigOptions, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs};
use streamfusion::bench::{
    collection_cast_function, flink_scalar_function, parameterized_scalar, registered_scalar_codes,
};
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_bridge::timestamp::timestamps_from_millis;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn literal(value: &str) -> ColumnarValue {
    ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.into())))
}
fn int(value: i32) -> ColumnarValue {
    ColumnarValue::Scalar(ScalarValue::Int32(Some(value)))
}
fn long(value: i64) -> ColumnarValue {
    ColumnarValue::Scalar(ScalarValue::Int64(Some(value)))
}
fn array(value: impl Array + 'static, rows: usize) -> ColumnarValue {
    ColumnarValue::Array(Arc::new(value).slice(1, rows))
}

fn invoke_fixture(op: i64, args: Vec<ColumnarValue>, rows: usize) -> ColumnarValue {
    let udf = flink_scalar_function(op, args.len());
    let fields: Vec<_> = args
        .iter()
        .map(|arg| Arc::new(Field::new("input", arg.data_type(), true)))
        .collect();
    let scalars: Vec<_> = args
        .iter()
        .map(|arg| match arg {
            ColumnarValue::Scalar(value) => Some(value),
            _ => None,
        })
        .collect();
    let field = udf
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &scalars,
        })
        .unwrap();
    udf.invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields: fields,
        number_rows: rows,
        return_field: field,
        config_options: Arc::new(ConfigOptions::new()),
    })
    .unwrap()
}

fn represented(input: &ColumnarValue, rows: usize) -> ColumnarValue {
    let input = input.to_array(rows).unwrap();
    let input = input.as_any().downcast_ref::<StringArray>().unwrap();
    let bytes = BinaryArray::from_iter(input.iter().map(|value| {
        value.map(|value| {
            value
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect::<Vec<_>>()
        })
    }));
    invoke_fixture(226, vec![ColumnarValue::Array(Arc::new(bytes))], rows)
}

fn represented_list(input: &ColumnarValue, rows: usize) -> ColumnarValue {
    let input = input.to_array(rows).unwrap();
    let input = input
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap();
    let values = represented(
        &ColumnarValue::Array(input.values().clone()),
        input.values().len(),
    )
    .to_array(input.values().len())
    .unwrap();
    ColumnarValue::Array(Arc::new(arrow::array::ListArray::new(
        Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        input.offsets().clone(),
        values,
        input.nulls().cloned(),
    )))
}

fn fixtures(rows: usize, nulls: bool, unicode: bool) -> Vec<(i64, Vec<ColumnarValue>)> {
    let valid = |row: usize| !nulls || row % 7 != 0;
    let text = if unicode {
        "é中🙂|alpha|beta".repeat(16)
    } else {
        "alpha|beta|ABC|".repeat(20)
    };
    let string = |value: &str| {
        array(
            StringArray::from_iter((0..rows + 1).map(|i| valid(i).then_some(value))),
            rows,
        )
    };
    let integers = array(
        Int64Array::from_iter((0..rows + 1).map(|i| valid(i).then_some(i as i64 - 10))),
        rows,
    );
    let dates = array(
        Date32Array::from_iter((0..rows + 1).map(|i| valid(i).then_some(i as i32 - 10))),
        rows,
    );
    let times: ColumnarValue = array(
        timestamps_from_millis(&Int64Array::from_iter(
            (0..rows + 1).map(|i| valid(i).then_some(i as i64 * 1000 - 10000)),
        )),
        rows,
    );
    let offsets = array(
        Int32Array::from_iter(
            (0..rows + 1).map(|i| valid(i + 1).then_some(if i % 2 == 0 { 3600 } else { -3600 })),
        ),
        rows,
    );
    let mut list = ListBuilder::new(Int64Builder::new());
    for row in 0..rows + 1 {
        for value in [
            Some(row as i64),
            None,
            Some(row as i64),
            Some(-(row as i64)),
        ] {
            list.values().append_option(value);
        }
        list.append(valid(row));
    }
    let lists = array(list.finish(), rows);
    let singletons = array(
        arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(
            (0..rows + 1).map(|row| {
                valid(row).then(|| {
                    if row % 3 == 0 {
                        vec![]
                    } else {
                        vec![(row % 5 != 0).then_some(row as i64)]
                    }
                })
            }),
        ),
        rows,
    );
    let mut map = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
    for row in 0..rows + 1 {
        for i in 0..4 {
            map.keys().append_value(i);
            map.values().append_option((i % 2 == 0).then_some(i));
        }
        map.append(valid(row)).unwrap();
    }
    let maps = array(map.finish(), rows);
    let mut strings = arrow::array::ListBuilder::new(arrow::array::StringBuilder::new());
    for row in 0..rows + 1 {
        for value in [Some(text.as_str()), None, Some("")] {
            strings.values().append_option(value);
        }
        strings.append(valid(row));
    }
    let string_lists = array(strings.finish(), rows);
    let mut cases = vec![
        (
            65,
            vec![array(
                Float64Array::from_iter((0..rows + 1).map(|i| {
                    valid(i).then_some(match i % 7 {
                        0 => -0.0,
                        1 => f64::NAN,
                        2 => f64::INFINITY,
                        _ => i as f64 - 100.0,
                    })
                })),
                rows,
            )],
        ),
        (67, vec![string(&text)]),
        (81, vec![integers.clone()]),
        (100, vec![string(&text), literal("alpha")]),
        (101, vec![string(&text), literal("beta")]),
        (102, vec![string(&text), literal("alpha")]),
        (103, vec![literal("alpha"), string(&text), int(1)]),
        (104, vec![integers.clone()]),
        (105, vec![integers.clone()]),
        (106, vec![string(&text)]),
        (107, vec![string(&text)]),
        (108, vec![string("00ff1234")]),
        (109, vec![integers.clone(), long(1)]),
        (110, vec![integers.clone(), long(1)]),
        (111, vec![string(&text)]),
        (112, vec![string(&text), literal("abc"), literal("ABC")]),
        (113, vec![string(&text)]),
        (114, vec![int(1), string(&text), literal("other")]),
        (115, vec![string(&text)]),
        (116, vec![string(&text), literal("ABC"), long(2), long(4)]),
        (117, vec![string("alpha%20beta+gamma")]),
        (118, vec![string("alpha%20beta+gamma")]),
        (120, vec![string(&text), literal("UTF-8")]),
        (
            121,
            vec![
                array(
                    BinaryArray::from_iter(
                        (0..rows + 1).map(|i| valid(i).then_some(text.as_bytes())),
                    ),
                    rows,
                ),
                literal("UTF-8"),
            ],
        ),
        (122, vec![string(&text)]),
        (123, vec![string("\"alpha\\n\"")]),
        (124, vec![string(&text), literal("|")]),
        (125, vec![string(&text), int(2), int(4)]),
        (126, vec![string(&text), int(4)]),
        (127, vec![string(&text), int(4)]),
        (128, vec![string(&text), int(320), literal(".")]),
        (129, vec![string(&text), int(320), literal(".")]),
        (130, vec![string(&text), literal("|"), int(1)]),
        (131, vec![string("2024-02-29")]),
        (133, vec![dates.clone()]),
        (134, vec![dates.clone()]),
        (135, vec![dates.clone()]),
        (136, vec![dates.clone()]),
        (139, vec![string(&text)]),
        (140, vec![string(&text)]),
        (
            142,
            vec![
                string("{\"a\":1}"),
                literal("strict $.a"),
                literal("FALSE"),
                literal("13.0"),
            ],
        ),
        (
            151,
            vec![array(
                BinaryArray::from_iter((0..rows + 1).map(|i| valid(i).then_some(text.as_bytes()))),
                rows,
            )],
        ),
        (152, vec![string(&text)]),
        (153, vec![literal("NULL"), literal("key"), string(&text)]),
        (
            154,
            vec![
                times.clone(),
                long(1000),
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
            ],
        ),
        (155, vec![times.clone()]),
        (156, vec![times.clone(), long(1000)]),
        (157, vec![times.clone(), int(1)]),
        (158, vec![integers.clone(), long(0)]),
        (159, vec![string("true")]),
        (160, vec![string("invalid")]),
        (161, vec![string(&text), literal("alpha"), int(1), int(1)]),
        (162, vec![string(&text)]),
        (165, vec![lists.clone()]),
        (166, vec![]),
        (167, vec![lists.clone()]),
        (169, vec![lists.clone()]),
        (170, vec![lists.clone(), int(-2), int(-1)]),
        (168, vec![lists.clone(), literal("ARRAY<BIGINT>")]),
        (171, vec![integers.clone(), long(42)]),
        (172, vec![times.clone(), times.clone()]),
        (173, vec![integers.clone()]),
        (174, vec![times.clone()]),
        (175, vec![times.clone(), int(1000)]),
        (176, vec![times.clone(), offsets.clone()]),
        (177, vec![times.clone(), offsets, int(1000)]),
        (178, vec![lists.clone(), long(1)]),
        (179, vec![lists.clone(), long(1)]),
        (180, vec![lists.clone()]),
        (181, vec![lists.clone()]),
        (182, vec![lists.clone(), long(1)]),
        (183, vec![lists.clone(), long(1)]),
        (184, vec![lists.clone(), long(1)]),
        (185, vec![lists.clone(), lists.clone()]),
        (186, vec![lists.clone(), lists.clone()]),
        (187, vec![lists.clone(), lists.clone()]),
        (188, vec![lists.clone(), lists.clone()]),
        (
            189,
            vec![lists.clone(), literal("ASC"), literal("NULLS FIRST")],
        ),
        (190, vec![singletons.clone()]),
        (201, vec![singletons]),
        (191, vec![lists.clone(), lists.clone()]),
        (192, vec![maps.clone()]),
        (193, vec![maps.clone()]),
        (194, vec![maps.clone()]),
        (195, vec![maps.clone(), maps.clone()]),
        (
            196,
            vec![integers.clone(), long(2), long(3), integers.clone()],
        ),
        (
            197,
            vec![literal("f0"), integers.clone(), literal("f1"), long(2)],
        ),
        (
            198,
            vec![literal("a"), integers.clone(), literal("b"), long(2)],
        ),
        (
            199,
            vec![literal("a"), integers.clone(), literal("b"), long(2)],
        ),
        (200, vec![string_lists.clone(), string("|"), string("NULL")]),
    ];
    for op in 203..=208 {
        cases.push((op, vec![dates.clone()]));
    }
    let boolean = |value| ColumnarValue::Scalar(ScalarValue::Boolean(Some(value)));
    cases.extend([
        (209, vec![times.clone(), boolean(false), boolean(false)]),
        (210, vec![times.clone(), long(86_400_000), boolean(false)]),
        (211, vec![dates.clone(), long(86_400_000), boolean(false)]),
        (
            212,
            vec![
                array(
                    arrow::array::Time32MillisecondArray::from_iter(
                        (0..rows + 1).map(|i| valid(i).then_some((i % 86_400) as i32 * 1000)),
                    ),
                    rows,
                ),
                long(3_600_000),
                boolean(true),
            ],
        ),
        (213, vec![dates, int(1), boolean(false)]),
        (214, vec![times.clone(), int(1), boolean(false)]),
        (215, vec![times.clone(), times.clone()]),
        (216, vec![times.clone(), times]),
        (217, vec![lists.clone(), boolean(true), boolean(false)]),
        (218, vec![integers.clone()]),
        (219, vec![integers.clone()]),
        (220, vec![string_lists.clone(), string_lists.clone()]),
        (
            221,
            vec![
                literal("x"),
                integers.clone(),
                literal("y"),
                represented(&string(&text), rows),
            ],
        ),
        (
            222,
            vec![
                literal("x"),
                integers.clone(),
                literal("y"),
                represented(&string(&text), rows),
            ],
        ),
        (
            223,
            vec![
                represented(&string(&text), rows),
                represented(&string("|"), rows),
            ],
        ),
        (224, vec![lists, boolean(true), boolean(false)]),
        (225, vec![represented(&string(&text), rows), integers]),
        (
            226,
            vec![array(
                BinaryArray::from_iter((0..rows + 1).map(|i| {
                    valid(i).then(|| {
                        text.encode_utf16()
                            .flat_map(u16::to_le_bytes)
                            .collect::<Vec<_>>()
                    })
                })),
                rows,
            )],
        ),
        (227, vec![maps]),
        (228, vec![represented_list(&string_lists, rows)]),
        (229, vec![represented_list(&string_lists, rows)]),
        (
            202,
            vec![
                represented(&string("{\"a\":1}"), rows),
                literal("strict $.a"),
                literal("NULL"),
                literal(""),
                literal("NULL"),
                literal(""),
                literal("13.0"),
            ],
        ),
    ]);
    for op in [144, 145, 146, 147] {
        cases.push((op, vec![string("{\"a\":1}"), literal("13.0")]));
    }
    for (op, document) in [
        (141, "{\"a\":\"text\"}"),
        (148, "{\"a\":true}"),
        (149, "{\"a\":123}"),
        (150, "{\"a\":12.5}"),
    ] {
        cases.push((
            op,
            vec![
                string(document),
                literal("strict $.a"),
                literal("NULL"),
                literal(""),
                literal("NULL"),
                literal(""),
                literal("13.0"),
            ],
        ));
    }
    let expected: BTreeSet<_> = registered_scalar_codes().into_iter().collect();
    assert_eq!(
        cases.iter().map(|(op, _)| *op).collect::<BTreeSet<_>>(),
        expected,
        "Every registered Flink scalar requires a benchmark fixture"
    );
    cases
}

fn parameterized(
    rows: usize,
    nulls: bool,
) -> Vec<(
    String,
    datafusion::logical_expr::ScalarUDF,
    Vec<ColumnarValue>,
)> {
    let valid = |i| !nulls || i % 7 != 0;
    let ints = array(
        Int64Array::from_iter((0..rows + 1).map(|i| valid(i).then_some(i as i64))),
        rows,
    );
    let decimals = array(
        Decimal128Array::from_iter((0..rows + 1).map(|i| valid(i).then_some(i as i128 * 12345)))
            .with_precision_and_scale(18, 2)
            .unwrap(),
        rows,
    );
    let floats = array(
        Float64Array::from_iter(
            (0..rows + 1).map(|i| valid(i).then_some(if i % 5 == 0 { f64::NAN } else { i as f64 })),
        ),
        rows,
    );
    let mut list = ListBuilder::new(Int64Builder::new());
    let mut map = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
    for i in 0..rows + 1 {
        for v in 0..4 {
            list.values().append_value(v);
            map.keys().append_value(v);
            map.values().append_option((v % 3 != 0).then_some(v));
        }
        list.append(valid(i));
        map.append(valid(i)).unwrap();
    }
    let list = array(list.finish(), rows);
    let map = array(map.finish(), rows);
    let mut cases = Vec::new();
    let mut add = |name: &str, ty: DataType, args: Vec<ColumnarValue>| {
        cases.push((name.to_string(), parameterized_scalar(name, ty), args))
    };
    for name in ["decimal_cast", "decimal_to_double", "decimal_to_float"] {
        add(name, decimals.data_type(), vec![decimals.clone()]);
    }
    for name in ["decimal_round", "decimal_truncate"] {
        add(name, decimals.data_type(), vec![decimals.clone(), int(1)]);
    }
    for name in ["decimal_add", "decimal_subtract", "decimal_multiply"] {
        add(
            name,
            decimals.data_type(),
            vec![decimals.clone(), decimals.clone()],
        );
    }
    add(
        "integer_divide",
        DataType::Int64,
        vec![ints.clone(), long(3)],
    );
    for name in ["integer_parse", "integer_try_parse"] {
        add(
            name,
            DataType::Int64,
            vec![array(
                StringArray::from_iter((0..rows + 1).map(|i| valid(i).then_some("12345678"))),
                rows,
            )],
        );
    }
    add("integer_format", DataType::Int64, vec![ints.clone()]);
    add("from_unixtime", DataType::Int64, vec![ints.clone()]);
    add("array_item", list.data_type(), vec![list, long(2)]);
    add("map_lookup_literal", map.data_type(), vec![map.clone()]);
    add(
        "map_lookup_dynamic",
        map.data_type(),
        vec![map.clone(), long(1)],
    );
    add(
        "float_comparison",
        DataType::Float64,
        vec![floats.clone(), floats],
    );
    add("random", DataType::Float64, vec![]);
    add("random_seeded", DataType::Float64, vec![int(42)]);
    add("random_integer", DataType::Int32, vec![int(100)]);
    add(
        "random_integer_seeded",
        DataType::Int32,
        vec![int(42), int(100)],
    );
    for name in [
        "current_timestamp",
        "current_date",
        "current_time",
        "unix_timestamp",
        "watermark",
    ] {
        add(name, DataType::Int64, vec![]);
    }
    cases.push((
        "op=195/unary".into(),
        flink_scalar_function(195, 1),
        vec![map],
    ));
    cases
}

fn collection_cases(
    rows: usize,
    nulls: bool,
) -> Vec<(
    String,
    datafusion::logical_expr::ScalarUDF,
    Vec<ColumnarValue>,
)> {
    let mut cases = Vec::new();
    for width in [8, 64] {
        let domain = if width == 8 { 4 } else { width };
        let valid = |row| !nulls || row % 7 != 0;
        let mut strings = ListBuilder::new(arrow::array::StringBuilder::new());
        let mut numbers = ListBuilder::new(arrow::array::Int32Builder::new());
        let mut parse = ListBuilder::new(arrow::array::StringBuilder::new());
        let mut multiset = MapBuilder::new(
            None,
            arrow::array::Int32Builder::new(),
            arrow::array::Int32Builder::new(),
        );
        for row in 0..=rows {
            if row % 11 != 0 {
                for col in 0..width {
                    let value = format!("é中\0{:04}", col % domain);
                    let present = !nulls || col % 9 != 0;
                    strings
                        .values()
                        .append_option(present.then_some(value.as_str()));
                    numbers
                        .values()
                        .append_option(present.then_some((col % domain) as i32));
                    parse
                        .values()
                        .append_option(present.then_some(if col == 3 && nulls {
                            "bad".to_owned()
                        } else {
                            (col % domain).to_string()
                        }));
                    multiset.keys().append_value((col % domain) as i32);
                    multiset.values().append_value((col % 3 + 1) as i32);
                }
            }
            strings.append(valid(row));
            numbers.append(valid(row));
            parse.append(valid(row));
            multiset.append(valid(row)).unwrap();
        }
        let strings = represented_list(&array(strings.finish(), rows), rows);
        let numbers = array(numbers.finish(), rows);
        let parse = represented_list(&array(parse.finish(), rows), rows);
        let multiset = array(multiset.finish(), rows);
        let needle = represented(
            &array(
                StringArray::from_iter((0..=rows).map(|row| valid(row).then_some("é中\0"))),
                rows,
            ),
            rows,
        );
        let flags = array(
            arrow::array::BooleanArray::from_iter(
                (0..=rows).map(|row| valid(row + 1).then_some(row % 2 == 0)),
            ),
            rows,
        );
        let map = invoke_fixture(191, vec![strings.clone(), numbers.clone()], rows);
        let mut add = |op, suffix: &str, args: Vec<ColumnarValue>| {
            cases.push((
                format!("represented/{op}/{suffix}/width={width}/domain={domain}"),
                flink_scalar_function(op, args.len()),
                args,
            ));
        };
        for op in [165, 169, 228, 229] {
            add(op, "unary", vec![strings.clone()]);
        }
        for op in [178, 179, 182, 183, 184] {
            add(op, "needle", vec![strings.clone(), needle.clone()]);
        }
        for op in [185, 220, 187, 188] {
            add(op, "set", vec![strings.clone(), strings.clone()]);
        }
        add(170, "slice", vec![strings.clone(), int(-3), int(-1)]);
        add(
            224,
            "runtime-flags",
            vec![strings.clone(), flags.clone(), flags],
        );
        add(
            191,
            "map-from-arrays",
            vec![strings.clone(), numbers.clone()],
        );
        for op in [192, 193, 194, 227] {
            add(op, "map-projection", vec![map.clone()]);
        }
        add(195, "map-union", vec![map.clone(), map.clone()]);
        add(225, "map-constructor", vec![needle.clone(), int(7)]);
        for op in [221, 222] {
            add(
                op,
                "row-object",
                vec![literal("x"), int(7), literal("y"), needle.clone()],
            );
        }
        for separator in [Some(""), Some("|"), None] {
            let separator_arg = represented(
                &array(StringArray::from_iter((0..=rows).map(|_| separator)), rows),
                rows,
            );
            add(
                223,
                &format!("split-{separator:?}"),
                vec![needle.clone(), separator_arg],
            );
        }
        let map_type = |key, value| {
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", key, false)),
                            Arc::new(Field::new("value", value, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        };
        for (name, input, target, nullable_failure) in [
            (
                if nulls { "try-array-int" } else { "array-int" },
                parse,
                DataType::new_list(DataType::Int32, true),
                nulls,
            ),
            ("array-string", numbers.clone(), DataType::Utf8, false),
            (
                "array-boolean",
                numbers,
                DataType::new_list(DataType::Boolean, true),
                false,
            ),
            (
                "map-bigint",
                map,
                map_type(DataType::Utf8, DataType::Int64),
                false,
            ),
            (
                "multiset-bigint",
                multiset,
                map_type(DataType::Int64, DataType::Int32),
                false,
            ),
        ] {
            cases.push((
                format!("represented/{name}/width={width}/domain={domain}"),
                collection_cast_function(target, nullable_failure),
                vec![input],
            ));
        }
    }
    cases
}

fn registry(c: &mut Criterion) {
    let classpath = std::env::var("SF_NATIVE_BENCH_CLASSPATH")
        .expect("Run bin/bench-native.py to prepare the SQL/JSON JVM classpath");
    let property = format!("-Djava.class.path={classpath}");
    let arguments = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&property)
        .option("-Xms128m")
        .option("-Xmx256m")
        .option("-Dfile.encoding=UTF-8")
        .option("-Duser.timezone=UTC")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(arguments).expect("Create the benchmark JVM");
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    header();
    let mut group = c.benchmark_group("scalar_registry");
    for rows in [16, 1024, 16384] {
        for (nulls, unicode) in [(false, false), (true, false), (true, true)] {
            group.throughput(Throughput::Elements(rows as u64));
            let mut cases: Vec<_> = fixtures(rows, nulls, unicode)
                .into_iter()
                .map(|(op, args)| {
                    (
                        format!("op={op}"),
                        flink_scalar_function(op, args.len()),
                        args,
                    )
                })
                .collect();
            if !unicode {
                cases.extend(parameterized(rows, nulls));
                cases.extend(collection_cases(rows, nulls));
            }
            for charset in ["UTF-16", "UTF-16BE", "UTF-16LE"] {
                let text = if unicode {
                    "\u{e9}\u{4e2d}\u{1f642}|alpha|beta".repeat(16)
                } else {
                    "alpha|beta".to_owned()
                };
                let mut bytes: Vec<u8> = text
                    .encode_utf16()
                    .flat_map(|unit| {
                        if charset == "UTF-16LE" {
                            unit.to_le_bytes()
                        } else {
                            unit.to_be_bytes()
                        }
                    })
                    .collect();
                bytes.extend_from_slice(if charset == "UTF-16LE" {
                    &[0xfe, 0xff]
                } else {
                    &[0xff, 0xfe]
                });
                bytes.push(0); // Trailing malformed byte after the runtime-sensitive code unit.
                let input = array(
                    BinaryArray::from_iter(
                        (0..rows + 1).map(|i| (!nulls || i % 7 != 0).then_some(bytes.as_slice())),
                    ),
                    rows,
                );
                for (jdk, replace) in [(11, true), (17, false)] {
                    cases.push((
                        format!("op=121/charset={charset}/jdk={jdk}"),
                        flink_scalar_function(121, 3),
                        vec![
                            input.clone(),
                            literal(charset),
                            ColumnarValue::Scalar(ScalarValue::Boolean(Some(replace))),
                        ],
                    ));
                }
            }
            for (jdk, version) in [(11, "10.0"), (17, "13.0")] {
                let documents = array(
                    StringArray::from_iter((0..rows + 1).map(|i| {
                        (!nulls || i % 7 != 0)
                            .then_some(["true\u{560}", r#"{"a":1}"#, "truea"][i % 3])
                    })),
                    rows,
                );
                cases.push((
                    format!("op=144/jdk={jdk}"),
                    flink_scalar_function(144, 2),
                    vec![documents, literal(version)],
                ));
            }
            for (case, udf, args) in cases {
                let types: Vec<_> = args.iter().map(ColumnarValue::data_type).collect();
                let fields: Vec<_> = types
                    .iter()
                    .map(|ty| Arc::new(Field::new("arg", ty.clone(), true)))
                    .collect();
                let scalars: Vec<_> = args
                    .iter()
                    .map(|arg| match arg {
                        ColumnarValue::Scalar(value) => Some(value),
                        _ => None,
                    })
                    .collect();
                let field = udf
                    .return_field_from_args(ReturnFieldArgs {
                        arg_fields: &fields,
                        scalar_arguments: &scalars,
                    })
                    .unwrap();
                let output_type = field.data_type().clone();
                let options = Arc::new(ConfigOptions::new());
                let run = || {
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: fields.clone(),
                        number_rows: rows,
                        return_field: field.clone(),
                        config_options: options.clone(),
                    })
                    .unwrap()
                };
                let label = format!(
                    "{case}/{}/{rows}/nulls={nulls}/unicode={unicode}",
                    udf.name()
                );
                run();
                let (output, allocations) = measure(run);
                let output = output.to_array(rows).unwrap();
                assert_eq!(output.len(), rows);
                assert_eq!(output.data_type(), &output_type);
                if case.starts_with("represented/try-array-int/") {
                    for row in 0..rows {
                        let source_row = row + 1;
                        assert_eq!(
                            output.is_null(row),
                            source_row % 11 != 0 || source_row % 7 == 0
                        );
                    }
                }
                if case.starts_with("represented/array-boolean/") {
                    let input = args[0].to_array(rows).unwrap();
                    let input = input
                        .as_any()
                        .downcast_ref::<arrow::array::ListArray>()
                        .unwrap();
                    let result = output
                        .as_any()
                        .downcast_ref::<arrow::array::ListArray>()
                        .unwrap();
                    assert_eq!(result.nulls(), input.nulls());
                    for row in 0..rows {
                        if input.is_null(row) {
                            continue;
                        }
                        let values = input.value(row);
                        let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
                        let actual = result.value(row);
                        let actual = actual
                            .as_any()
                            .downcast_ref::<arrow::array::BooleanArray>()
                            .unwrap();
                        assert_eq!(
                            actual.iter().collect::<Vec<_>>(),
                            values
                                .iter()
                                .map(|value| value.map(|value| value != 0))
                                .collect::<Vec<_>>()
                        );
                    }
                }
                let inputs: Vec<_> = args
                    .iter()
                    .filter_map(|arg| match arg {
                        ColumnarValue::Array(array) => Some(array.clone()),
                        _ => None,
                    })
                    .collect();
                report(&label, &inputs, &[output], allocations);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(run()))
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, registry);
criterion_main!(benches);
