//! Complete production Calc on prepared Arrow; timing and output consumption stay inside Rust.
use arrow::{
    array::{Array, Decimal128Array, Int64Array, ListArray},
    ipc::reader::FileReader,
    record_batch::RecordBatch,
};
use std::{fs::File, hint::black_box, time::Instant};
use streamfusion::bench::CalcProgram;

fn consume(batch: &RecordBatch) -> (usize, i64) {
    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = list.values();
    let decimal = values.as_any().downcast_ref::<Decimal128Array>();
    let integer = values.as_any().downcast_ref::<Int64Array>();
    let mut hash = 0_i64;
    for row in 0..list.len() {
        let mut h = -1_i64;
        if list.is_valid(row) {
            let start = list.value_offsets()[row] as usize;
            let end = list.value_offsets()[row + 1] as usize;
            h = (end - start + 1) as i64;
            for index in start..end {
                let value = if values.is_null(index) {
                    0
                } else if let Some(values) = decimal {
                    values.value(index) as i64
                } else {
                    integer.unwrap().value(index)
                };
                h = h.wrapping_mul(31).wrapping_add(value);
            }
        }
        hash = hash.wrapping_add(h);
    }
    black_box((batch.num_rows(), hash))
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert!(
        args.len() >= 2,
        "array_cast INPUT.arrow [warmups=3] [runs=7]"
    );
    let warmups: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(3);
    let runs: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(7);
    let schema = std::fs::read_to_string(format!("{}.target", args[1])).unwrap();
    let input = FileReader::try_new(File::open(&args[1]).unwrap(), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let batches: Vec<_> = (0..input.num_rows())
        .step_by(1024)
        .map(|offset| input.slice(offset, 1024.min(input.num_rows() - offset)))
        .collect();
    for trial in 0..warmups + runs {
        let mut calc = CalcProgram::new(
            vec![43, 0],
            vec![0, 0],
            vec![1, 0],
            vec![],
            vec![],
            vec![Some(schema.clone())],
        );
        calc.run(batches[0].clone());
        let (mut rows, mut hash) = (0, 0_i64);
        let start = Instant::now();
        for batch in &batches {
            let output = calc.run(black_box(batch.clone()));
            let (count, digest) = consume(&output);
            rows += count;
            hash = hash.wrapping_add(digest);
        }
        black_box((rows, hash));
        let seconds = start.elapsed().as_secs_f64();
        println!("{{\"trial\":{trial},\"warmup\":{},\"seconds\":{seconds:.9},\"rows\":{rows},\"checksum\":{hash}}}",trial<warmups);
    }
}
