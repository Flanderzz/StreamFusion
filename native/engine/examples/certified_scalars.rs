//! Prepared Arrow inputs; production certificate, computation and output consumption stay timed.
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion_bridge::timestamp::{
    timestamp_array, timestamp_type, TimestampColumn, TimestampValue,
};

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let count: usize = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(131072);
    let warmups: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(3);
    let runs: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(5);
    let bytes: usize = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(264);
    let url_cardinality: usize = args.get(5).map(|s| s.parse().unwrap()).unwrap_or(1);
    assert!((1..=1024).contains(&url_cardinality));
    let payload = (" abC def_09 ".repeat(bytes / 12 + 1))[..bytes].to_owned();
    let text = Arc::new(StringArray::from_iter_values(
        (0..1024).map(|_| payload.as_str()),
    )) as ArrayRef;
    let constant =
        |s: &str| Arc::new(StringArray::from_iter_values((0..1024).map(|_| s))) as ArrayRef;
    let url = format!("http://example.org/{payload}").replace(' ', "x");
    let urls = Arc::new(StringArray::from_iter_values((0..1024).map(|i| {
        if url_cardinality == 1 {
            url.clone()
        } else {
            format!("{url}/{}", i % url_cardinality)
        }
    }))) as ArrayRef;
    let numbers = Arc::new(Int64Array::from_iter_values((0..1024).map(|i| i - 512))) as ArrayRef;
    let dates = Arc::new(Date32Array::from_iter_values((0..1024).map(|i| 10957 + i))) as ArrayRef;
    let timestamps = Arc::new(timestamp_array((0..1024).map(|i| {
        Some(TimestampValue::new((10957 + i) * 86_400_000 + 45_296_123, 456789).unwrap())
    }))) as ArrayRef;
    let booleans = |b| Arc::new(BooleanArray::from(vec![b; 1024])) as ArrayRef;
    let cases = [
        (
            "REGEXP_COUNT",
            4,
            vec![text.clone(), constant("a")],
            DataType::Int32,
        ),
        (
            "LIKE_ESCAPE",
            7,
            vec![text.clone(), constant("%!_%"), constant("!")],
            DataType::Boolean,
        ),
        (
            "SIMILAR",
            8,
            vec![text.clone(), constant(".*")],
            DataType::Boolean,
        ),
        ("PARSE_URL", 9, vec![urls, constant("PATH")], DataType::Utf8),
        (
            "PRINTF",
            10,
            vec![constant("n=%020d"), numbers],
            DataType::Utf8,
        ),
        ("YEAR_DATE", 203, vec![dates], DataType::Int64),
        (
            "FLOOR_MONTH",
            209,
            vec![timestamps.clone(), booleans(false), booleans(false)],
            timestamp_type(),
        ),
        (
            "ADD_MONTH",
            214,
            vec![
                timestamps.clone(),
                Arc::new(Int32Array::from(vec![1; 1024])),
                booleans(false),
            ],
            timestamp_type(),
        ),
        (
            "GREATEST_TIMESTAMP",
            215,
            vec![timestamps.clone(), timestamps],
            timestamp_type(),
        ),
    ];
    for (name, op, arrays, target) in cases {
        let batches = (0..count)
            .step_by(1024)
            .map(|start| {
                let rows = (count - start).min(1024);
                arrays.iter().map(|a| a.slice(0, rows)).collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let udf = (op >= 200).then(|| streamfusion::bench::flink_scalar_function(op, arrays.len()));
        let return_field = Arc::new(Field::new("result", target.clone(), true));
        let arg_fields: Vec<_> = arrays
            .iter()
            .map(|a| Arc::new(Field::new("arg", a.data_type().clone(), true)))
            .collect();
        let options = Arc::new(ConfigOptions::new());
        for trial in 0..warmups + runs {
            let mut checksum = 0_i64;
            let timer = Instant::now();
            for batch in &batches {
                let output = if let Some(udf) = &udf {
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: batch.iter().cloned().map(ColumnarValue::Array).collect(),
                        arg_fields: arg_fields.clone(),
                        number_rows: batch[0].len(),
                        return_field: return_field.clone(),
                        config_options: options.clone(),
                    })
                    .unwrap()
                    .into_array(batch[0].len())
                    .unwrap()
                } else {
                    streamfusion::bench::certified_builtin(op, black_box(batch), &target)
                };
                if let Some(strings) = output.as_any().downcast_ref::<StringArray>() {
                    for value in strings.iter() {
                        checksum = checksum.wrapping_add(value.map_or(-1, |s| {
                            s.bytes()
                                .fold(0_i64, |h, b| h.wrapping_mul(31).wrapping_add(i64::from(b)))
                        }));
                    }
                } else if let Some(values) = output.as_any().downcast_ref::<Int32Array>() {
                    checksum += values.iter().flatten().map(i64::from).sum::<i64>();
                } else if let Some(values) = output.as_any().downcast_ref::<BooleanArray>() {
                    checksum += values.iter().flatten().map(i64::from).sum::<i64>();
                } else if let Some(values) = output.as_any().downcast_ref::<Int64Array>() {
                    checksum += values.iter().flatten().sum::<i64>();
                } else if streamfusion_bridge::timestamp::is_timestamp(output.data_type()) {
                    let values = TimestampColumn::try_new(output.as_ref()).unwrap();
                    for row in 0..output.len() {
                        if !values.is_null(row) {
                            let value = values.value(row).unwrap();
                            checksum = checksum
                                .wrapping_add(value.millis())
                                .wrapping_add(i64::from(value.nano_of_milli()));
                        }
                    }
                }
            }
            black_box(checksum);
            println!(
                "{name},rust,{bytes},{count},{trial},{},{:.9},{checksum},{url_cardinality}",
                trial < warmups,
                timer.elapsed().as_secs_f64()
            );
        }
    }
}
