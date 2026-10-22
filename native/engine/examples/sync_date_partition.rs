//! Production Calc timing starts in Rust on the same prepared rows as the stock Flink driver.
use arrow::{
    array::{Array, StringArray},
    ipc::{reader::FileReader, writer::FileWriter},
    record_batch::RecordBatch,
};
use serde::Deserialize;
use std::{fs::File, hint::black_box, time::Instant};
use streamfusion::bench::CalcProgram;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Program {
    kinds: Vec<i64>,
    payload: Vec<i64>,
    child_counts: Vec<i64>,
    longs: Vec<i64>,
    doubles: Vec<f64>,
    strings: Vec<Option<String>>,
    projection_roots: Vec<usize>,
    condition_root: i64,
    output_names: Vec<String>,
}

fn program(path: &str) -> CalcProgram {
    let plan: Program = serde_json::from_reader(File::open(path).unwrap()).unwrap();
    assert_eq!(plan.condition_root, -1);
    assert!(!plan
        .kinds
        .iter()
        .any(|kind| matches!(kind, 17 | 38 | 41 | 42)));
    CalcProgram::new(
        plan.kinds,
        plan.payload,
        plan.child_counts,
        plan.longs,
        plan.doubles,
        plan.strings,
    )
    .with_projections(plan.projection_roots, plan.output_names)
}

fn consume(batch: &RecordBatch) -> i64 {
    let mut hash = 0i64;
    for column in &batch.columns()[batch.num_columns() - 3..] {
        let values = column.as_any().downcast_ref::<StringArray>().unwrap();
        for value in values.iter() {
            let value = value.map_or(-1, |text| {
                text.bytes()
                    .fold(0i64, |h, b| h.wrapping_mul(31).wrapping_add(i64::from(b)))
            });
            hash = hash.wrapping_add(value);
        }
    }
    black_box(hash)
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert!(
        args.len() >= 2,
        "sync_date_partition INPUT.arrow [warmups=5] [runs=9] [output.arrow]"
    );
    let warmups: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(5);
    let runs: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(9);
    let input = FileReader::try_new(File::open(&args[1]).unwrap(), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let batches = (0..input.num_rows())
        .step_by(1024)
        .map(|start| input.slice(start, 1024.min(input.num_rows() - start)))
        .collect::<Vec<_>>();
    let path = format!("{}.plan.json", args[1]);
    if let Some(path_out) = args.get(4) {
        let mut calc = program(&path);
        let first = calc.run(batches[0].clone());
        let mut writer =
            FileWriter::try_new(File::create(path_out).unwrap(), first.schema().as_ref()).unwrap();
        writer.write(&first).unwrap();
        for batch in &batches[1..] {
            writer.write(&calc.run(batch.clone())).unwrap();
        }
        writer.finish().unwrap();
    }
    for trial in 0..warmups + runs {
        let mut calc = program(&path);
        calc.run(batches[0].clone());
        let (mut rows, mut hash) = (0, 0i64);
        let clock = Instant::now();
        for batch in &batches {
            let output = calc.run(black_box(batch.clone()));
            rows += output.num_rows();
            hash = hash.wrapping_add(consume(&output));
        }
        black_box((rows, hash));
        let seconds = clock.elapsed().as_secs_f64();
        println!(
            "{{\"trial\":{trial},\"warmup\":{},\"seconds\":{seconds:.9},\"rows\":{rows},\"checksum\":{hash}}}",
            trial < warmups
        );
    }
}
