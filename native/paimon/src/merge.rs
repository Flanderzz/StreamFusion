// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Arrow cursor/key comparison and interleave output adapted from apache/paimon-rust
//! 6824487813c69b1d4975e1add8c37343b3ead75b, table/sort_merge.rs (Apache-2.0).
//! Keep only the current winner for a key; deleted keys never pin completed input batches.
use crate::loser_tree::LoserTree;
use arrow::array::{Array, Int64Array, Int8Array};
use arrow::compute::interleave;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

type Result<T> = std::result::Result<T, String>;
pub(crate) type Pull<'a> = dyn FnMut(usize) -> Result<Option<RecordBatch>> + 'a;

struct Cursor {
    batch: Arc<RecordBatch>,
    keys: Rows,
    row: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::VecDeque;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("seq", DataType::Int64, false),
            Field::new("kind", DataType::Int8, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }
    fn batch(rows: &[(i32, i64, i8, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(Int8Array::from_iter_values(rows.iter().map(|r| r.2))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3))),
            ],
        )
        .unwrap()
    }
    fn merger(runs: usize, rows: usize, budget: usize) -> Merger {
        let output = Arc::new(Schema::new(vec![
            schema().field(3).clone(),
            schema().field(2).clone(),
        ]));
        Merger::new(schema(), output, 1, runs, rows, budget).unwrap()
    }

    #[test]
    fn sorted_streams_merge_versions_deletes_and_batch_boundaries() {
        let mut inputs: Vec<VecDeque<_>> = vec![
            vec![
                batch(&[]),
                batch(&[(1, 1, 0, "old"), (2, 1, 0, "removed")]),
                batch(&[(4, 1, 0, "four")]),
            ]
            .into(),
            vec![
                batch(&[(1, 2, 2, "new")]),
                batch(&[(2, 3, 3, "deleted"), (3, 1, 0, "three")]),
            ]
            .into(),
            vec![
                batch(&[(1, 4, 2, "latest"), (3, 2, 1, "before")]),
                batch(&[(3, 3, 2, "after")]),
            ]
            .into(),
        ];
        let mut merger = merger(3, 2, 1 << 20);
        let mut output = Vec::new();
        while let Some(batch) = merger.next(&mut |run| Ok(inputs[run].pop_front())).unwrap() {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let kinds = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                output.push((values.value(row).to_string(), kinds.value(row)));
            }
        }
        assert_eq!(
            output,
            vec![
                ("latest".into(), 2),
                ("after".into(), 2),
                ("four".into(), 0)
            ]
        );
    }

    #[test]
    fn hot_key_and_delete_only_input_reclaim_completed_batches() {
        for one_key in [false, true] {
            let mut merger = merger(1, 4096, 32 * 1024);
            let mut next = 0;
            let result = merger
                .next(&mut |_| {
                    next += 1;
                    Ok((next <= 100_000).then(|| {
                        batch(&[(if one_key { 0 } else { next }, next as i64, 3, "deleted")])
                    }))
                })
                .unwrap();
            assert!(result.is_none());
            assert!(merger.peak_bytes < 16 * 1024, "{}", merger.peak_bytes);
        }
    }

    #[test]
    fn retained_output_flushes_by_bytes_before_row_limit() {
        let mut merger = merger(1, 4096, 256 * 1024);
        let mut next = 0;
        let value = "x".repeat(8192);
        let mut pull = |_| {
            next += 1;
            Ok((next <= 128).then(|| batch(&[(next, next as i64, 0, &value)])))
        };
        let mut count = 0;
        let mut batches = 0;
        while let Some(batch) = merger.next(&mut pull).unwrap() {
            count += batch.num_rows();
            batches += 1;
        }
        assert_eq!(count, 128);
        assert!(batches > 1);
        assert!(merger.peak_bytes < 256 * 1024);
    }

    #[test]
    fn input_budget_and_callback_errors_fail_the_read() {
        let mut limited = merger(1, 4096, 1024);
        assert!(limited
            .next(&mut |_| Ok(Some(batch(&[(1, 1, 0, &"x".repeat(2048))]))))
            .unwrap_err()
            .contains("exceeding budget"));
        let mut failed = merger(1, 4096, 1 << 20);
        assert_eq!(
            failed
                .next(&mut |_| Err("storage failure".into()))
                .unwrap_err(),
            "storage failure"
        );
    }
}

pub(crate) struct Merger {
    cursors: Vec<Option<Cursor>>,
    tree: LoserTree,
    converter: RowConverter,
    key_count: usize,
    output: SchemaRef,
    columns: Vec<usize>,
    batch_rows: usize,
    budget: usize,
    initialized: bool,
    pub peak_bytes: usize,
}

impl Merger {
    pub fn new(
        input: SchemaRef,
        output: SchemaRef,
        key_count: usize,
        runs: usize,
        batch_rows: usize,
        budget: usize,
    ) -> Result<Self> {
        if runs == 0 || batch_rows == 0 || budget == 0 || key_count + 2 > input.fields().len() {
            return Err("Invalid snapshot merger configuration".into());
        }
        let converter = RowConverter::new(
            input.fields()[..key_count]
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )
        .map_err(|e| e.to_string())?;
        let mut columns: Vec<usize> = (key_count + 2..input.fields().len()).collect();
        columns.push(key_count + 1);
        if columns.len() != output.fields().len() {
            return Err("Snapshot output schema does not match input values and kind".into());
        }
        Ok(Self {
            cursors: (0..runs).map(|_| None).collect(),
            tree: LoserTree::new(runs),
            converter,
            key_count,
            output,
            columns,
            batch_rows,
            budget,
            initialized: false,
            peak_bytes: 0,
        })
    }

    fn compare(cursors: &[Option<Cursor>], a: usize, b: usize) -> Ordering {
        match (&cursors[a], &cursors[b]) {
            (None, None) => Ordering::Equal,
            (None, _) => Ordering::Greater,
            (_, None) => Ordering::Less,
            (Some(a), Some(b)) => a.keys.row(a.row).cmp(&b.keys.row(b.row)),
        }
    }

    fn refill(&mut self, run: usize, pull: &mut Pull<'_>) -> Result<()> {
        self.cursors[run] = None;
        while let Some(batch) = pull(run)? {
            if batch.num_rows() == 0 {
                continue;
            }
            let keys = self
                .converter
                .convert_columns(&batch.columns()[..self.key_count])
                .map_err(|e| e.to_string())?;
            self.cursors[run] = Some(Cursor {
                batch: Arc::new(batch),
                keys,
                row: 0,
            });
            break;
        }
        Ok(())
    }

    fn retained(
        &mut self,
        batches: &[Arc<RecordBatch>],
        winner: Option<&Arc<RecordBatch>>,
    ) -> Result<()> {
        let mut unique = HashMap::new();
        for batch in batches.iter().chain(winner) {
            unique.insert(Arc::as_ptr(batch), batch.get_array_memory_size());
        }
        let mut key_bytes = self.converter.size();
        for c in self.cursors.iter().flatten() {
            unique.insert(Arc::as_ptr(&c.batch), c.batch.get_array_memory_size());
            key_bytes = key_bytes.saturating_add(c.keys.size());
        }
        let bytes = unique.values().fold(key_bytes, |n, b| n.saturating_add(*b));
        self.peak_bytes = self.peak_bytes.max(bytes);
        if bytes > self.budget {
            return Err(format!(
                "Paimon snapshot merge retained {bytes} bytes, exceeding budget {}",
                self.budget
            ));
        }
        Ok(())
    }

    pub fn next(&mut self, pull: &mut Pull<'_>) -> Result<Option<RecordBatch>> {
        if !self.initialized {
            for run in 0..self.cursors.len() {
                self.refill(run, pull)?;
                self.retained(&[], None)?;
            }
            self.tree
                .init(|a, b| Self::compare(&self.cursors, a, b).then(a.cmp(&b)).is_gt());
            self.initialized = true;
        }
        let mut batches: Vec<Arc<RecordBatch>> = Vec::new();
        let mut batch_ids = HashMap::new();
        let mut indices = Vec::new();
        let mut output_bytes = 0usize;
        while let Some(cursor) = &self.cursors[self.tree.winner()] {
            let key = cursor.keys.row(cursor.row).owned();
            let mut best: Option<(Arc<RecordBatch>, usize, i64)> = None;
            loop {
                let run = self.tree.winner();
                let Some(cursor) = &mut self.cursors[run] else {
                    break;
                };
                if cursor.keys.row(cursor.row) != key.row() {
                    break;
                }
                let sequence = cursor
                    .batch
                    .column(self.key_count)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("Snapshot sequence column is not Int64")?;
                if sequence.is_null(cursor.row) {
                    return Err("Null snapshot sequence".into());
                }
                let seq = sequence.value(cursor.row);
                if best
                    .as_ref()
                    .is_none_or(|(_, _, previous)| seq >= *previous)
                {
                    best = Some((cursor.batch.clone(), cursor.row, seq));
                }
                cursor.row += 1;
                if cursor.row == cursor.batch.num_rows() {
                    self.refill(run, pull)?;
                    self.retained(&batches, best.as_ref().map(|(b, _, _)| b))?;
                }
                self.tree
                    .update(|a, b| Self::compare(&self.cursors, a, b).then(a.cmp(&b)).is_gt());
            }
            let (batch, row, _) = best.expect("nonempty key group");
            let kinds = batch
                .column(self.key_count + 1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or("Snapshot kind column is not Int8")?;
            if kinds.is_null(row) {
                return Err("Null snapshot row kind".into());
            }
            match kinds.value(row) {
                0 | 2 => {
                    let id = *batch_ids.entry(Arc::as_ptr(&batch)).or_insert_with(|| {
                        let id = batches.len();
                        output_bytes = output_bytes.saturating_add(batch.get_array_memory_size());
                        batches.push(batch);
                        id
                    });
                    indices.push((id, row));
                }
                1 | 3 => {}
                kind => return Err(format!("Invalid Paimon row kind {kind}")),
            }
            if indices.len() >= self.batch_rows || output_bytes >= self.budget / 4 {
                break;
            }
        }
        if indices.is_empty() {
            return Ok(None);
        }
        self.retained(&batches, None)?;
        let columns = self
            .columns
            .iter()
            .map(|&column| {
                let arrays: Vec<&dyn Array> =
                    batches.iter().map(|b| b.column(column).as_ref()).collect();
                interleave(&arrays, &indices).map_err(|e| e.to_string())
            })
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(self.output.clone(), columns)
            .map(Some)
            .map_err(|e| e.to_string())
    }
}
