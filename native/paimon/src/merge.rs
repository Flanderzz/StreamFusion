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
//! Sequence/first-row/delete policies follow Paimon 2.0.0 UserDefinedSeqComparator,
//! DeduplicateMergeFunction and FirstRowMergeFunction.
//! Selection keeps one winner; partial-update keeps one source cell per column.
//! Deleted keys never pin completed input batches.
use crate::loser_tree::LoserTree;
use crate::partial_update::PartialUpdate;
use arrow::array::{new_null_array, Array, ArrayRef, Int64Array, Int8Array};
use arrow::compute::interleave;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use streamfusion_bridge::ordering::canonical_ordering_column;

pub(crate) type Result<T> = std::result::Result<T, String>;
pub(crate) type Pull<'a> = dyn FnMut(usize) -> Result<Option<RecordBatch>> + 'a;

pub(crate) struct Cursor {
    pub batch: Arc<RecordBatch>,
    keys: Rows,
    sequences: Option<Rows>,
    pub row: usize,
}

pub(crate) struct Options {
    pub sequence_columns: Vec<usize>,
    pub sequence_ascending: bool,
    pub first_row: bool,
    pub ignore_delete: bool,
    pub partial_update: bool,
    pub remove_on_delete: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            sequence_columns: Vec::new(),
            sequence_ascending: true,
            first_row: false,
            ignore_delete: false,
            partial_update: false,
            remove_on_delete: false,
        }
    }
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
        Merger::new(schema(), output, 1, runs, rows, budget, Options::default()).unwrap()
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
    fn repeated_keys_in_one_run_follow_java_group_boundaries_without_retaining_old_batches() {
        for first_row in [false, true] {
            let mut merger = merger(1, 4096, 32 * 1024);
            merger.options.first_row = first_row;
            merger.options.ignore_delete = true;
            let mut next = 0;
            let mut count = 0;
            while let Some(result) = merger
                .next(&mut |_| {
                    next += 1;
                    Ok((next <= 100_000)
                        .then(|| batch(&[(1, next, if next % 3 == 0 { 2 } else { 3 }, "value")])))
                })
                .unwrap()
            {
                count += result.num_rows();
            }
            assert_eq!(count, 33_333);
            assert!(merger.peak_bytes < 32 * 1024, "{}", merger.peak_bytes);
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
    fn partial_update_delete_only_groups_reclaim_completed_batches() {
        let mut merger = merger(3, 4096, 32 * 1024);
        merger.partial = Some(PartialUpdate::new(4, 3, false, true));
        let mut counts = [0; 3];
        let output = merger
            .next(&mut |run| {
                counts[run] += 1;
                Ok((counts[run] <= 10_000).then(|| batch(&[(counts[run], 0, 3, "deleted")])))
            })
            .unwrap();
        assert!(output.is_none());
        assert_eq!(counts, [10_001; 3]);
        assert!(merger.peak_bytes < 16 * 1024, "{}", merger.peak_bytes);
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
    sequence_converter: Option<RowConverter>,
    options: Options,
    key_count: usize,
    partial: Option<PartialUpdate>,
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
        options: Options,
    ) -> Result<Self> {
        if runs == 0
            || batch_rows == 0
            || budget == 0
            || key_count + 2 > input.fields().len()
            || output.fields().is_empty()
        {
            return Err("Invalid snapshot merger configuration".into());
        }
        let converter = RowConverter::new(
            input.fields()[..key_count]
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )
        .map_err(|e| e.to_string())?;
        let sequence_converter = if options.sequence_columns.is_empty() {
            None
        } else {
            Some(
                RowConverter::new(
                    options
                        .sequence_columns
                        .iter()
                        .map(|&column| {
                            SortField::new_with_options(
                                input.field(column).data_type().clone(),
                                arrow::compute::SortOptions {
                                    descending: !options.sequence_ascending,
                                    nulls_first: true,
                                },
                            )
                        })
                        .collect(),
                )
                .map_err(|e| e.to_string())?,
            )
        };
        let mut columns = output.fields()[..output.fields().len() - 1]
            .iter()
            .map(|field| {
                (key_count + 2..input.fields().len())
                    .find(|&i| input.field(i).name() == field.name())
                    .ok_or_else(|| format!("Missing snapshot output column {}", field.name()))
            })
            .collect::<Result<Vec<_>>>()?;
        columns.push(key_count + 1);
        Ok(Self {
            cursors: (0..runs).map(|_| None).collect(),
            tree: LoserTree::new(runs),
            converter,
            sequence_converter,
            partial: options.partial_update.then(|| {
                PartialUpdate::new(
                    input.fields().len(),
                    key_count + 2,
                    options.ignore_delete,
                    options.remove_on_delete,
                )
            }),
            options,
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
            let sequence = batch
                .column(self.key_count)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("Snapshot sequence column is not Int64")?;
            if sequence.null_count() != 0 {
                return Err("Null snapshot sequence".into());
            }
            let key_columns = batch.columns()[..self.key_count]
                .iter()
                .map(canonical_ordering_column)
                .collect::<Vec<_>>();
            let keys = self
                .converter
                .convert_columns(&key_columns)
                .map_err(|e| e.to_string())?;
            let sequences = self
                .sequence_converter
                .as_mut()
                .map(|converter| {
                    let columns = self
                        .options
                        .sequence_columns
                        .iter()
                        .map(|&i| canonical_ordering_column(batch.column(i)))
                        .collect::<Vec<_>>();
                    converter
                        .convert_columns(&columns)
                        .map_err(|e| e.to_string())
                })
                .transpose()?;
            self.cursors[run] = Some(Cursor {
                batch: Arc::new(batch),
                keys,
                sequences,
                row: 0,
            });
            break;
        }
        Ok(())
    }

    fn retained(&mut self, batches: &[Arc<RecordBatch>]) -> Result<()> {
        let mut unique = HashMap::new();
        for batch in batches {
            unique.insert(Arc::as_ptr(batch), batch.get_array_memory_size());
        }
        let mut key_bytes = self.converter.size()
            + self
                .sequence_converter
                .as_ref()
                .map_or(0, RowConverter::size);
        for c in self.cursors.iter().flatten() {
            unique.insert(Arc::as_ptr(&c.batch), c.batch.get_array_memory_size());
            key_bytes = key_bytes.saturating_add(c.keys.size());
            key_bytes = key_bytes.saturating_add(c.sequences.as_ref().map_or(0, Rows::size));
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

    fn compare_sequence(cursors: &[Option<Cursor>], keys: usize, a: usize, b: usize) -> Ordering {
        match (&cursors[a], &cursors[b]) {
            (None, _) => Ordering::Greater,
            (_, None) => Ordering::Less,
            (Some(a), Some(b)) => {
                let user = a
                    .sequences
                    .as_ref()
                    .map(|s| s.row(a.row))
                    .cmp(&b.sequences.as_ref().map(|s| s.row(b.row)));
                user.then_with(|| {
                    let value = |c: &Cursor| {
                        c.batch
                            .column(keys)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("snapshot sequence Int64")
                            .value(c.row)
                    };
                    value(a).cmp(&value(b))
                })
            }
        }
    }

    fn advance_tree(&mut self, run: usize) {
        self.tree.advance(
            run,
            |a, b| Self::compare(&self.cursors, a, b),
            |a, b| Self::compare_sequence(&self.cursors, self.key_count, a, b),
        );
    }

    pub fn next(&mut self, pull: &mut Pull<'_>) -> Result<Option<RecordBatch>> {
        if !self.initialized {
            // Released Java builds in reverse order; equal-sequence winners depend on this.
            for run in (0..self.cursors.len()).rev() {
                self.refill(run, pull)?;
                self.retained(&[])?;
                self.advance_tree(run);
            }
            self.initialized = true;
        }
        let mut batches: Vec<Arc<RecordBatch>> = Vec::new();
        let mut batch_ids = HashMap::new();
        let mut indices = Vec::new();
        let mut output_bytes = 0usize;
        let mut group = Vec::with_capacity(self.cursors.len());
        let mut cell_indices = vec![
            Vec::new();
            if self.partial.is_some() {
                self.columns.len() - 1
            } else {
                0
            }
        ];
        let mut output_kinds = Vec::new();
        loop {
            // Java advances popped leaves only after reducing the entire current group.
            while self.tree.popped() {
                let run = self.tree.winner();
                let cursor = self.cursors[run].as_mut().expect("popped cursor");
                cursor.row += 1;
                if cursor.row == cursor.batch.num_rows() {
                    self.refill(run, pull)?;
                    self.retained(&batches)?;
                }
                self.advance_tree(run);
            }
            if self.cursors[self.tree.winner()].is_none() {
                break;
            }
            let mut best = None;
            group.clear();
            while !self.tree.popped() {
                let run = self.tree.winner();
                let cursor = self.cursors[run].as_ref().expect("group cursor");
                let kind = Self::kind(&cursor.batch, self.key_count, cursor.row)?;
                if self.partial.is_some() {
                    group.push((run, kind));
                }
                if !(self.options.ignore_delete && matches!(kind, 1 | 3))
                    && (!self.options.first_row || best.is_none())
                {
                    best = Some(run);
                }
                self.tree.pop(
                    |a, b| Self::compare(&self.cursors, a, b),
                    |a, b| Self::compare_sequence(&self.cursors, self.key_count, a, b),
                );
            }
            if let Some(partial) = &mut self.partial {
                let kind = partial.select(&group, &self.cursors)?;
                if matches!(kind, 0 | 2) {
                    for (i, &column) in self.columns[..self.columns.len() - 1].iter().enumerate() {
                        let index = if let Some(run) = partial.cells[column - self.key_count - 2] {
                            let cursor = self.cursors[run].as_ref().expect("selected cell cursor");
                            let batch = &cursor.batch;
                            let id = *batch_ids.entry(Arc::as_ptr(batch)).or_insert_with(|| {
                                let id = batches.len();
                                output_bytes =
                                    output_bytes.saturating_add(batch.get_array_memory_size());
                                batches.push(batch.clone());
                                id
                            });
                            (id, cursor.row)
                        } else {
                            (usize::MAX, 0)
                        };
                        cell_indices[i].push(index);
                    }
                    output_kinds.push(kind);
                }
            } else if let Some(run) = best {
                let cursor = self.cursors[run].as_ref().expect("winner cursor");
                if matches!(
                    Self::kind(&cursor.batch, self.key_count, cursor.row)?,
                    0 | 2
                ) {
                    let batch = &cursor.batch;
                    let id = *batch_ids.entry(Arc::as_ptr(batch)).or_insert_with(|| {
                        let id = batches.len();
                        output_bytes = output_bytes.saturating_add(batch.get_array_memory_size());
                        batches.push(batch.clone());
                        id
                    });
                    indices.push((id, cursor.row));
                }
            }
            if indices.len().max(output_kinds.len()) >= self.batch_rows
                || output_bytes >= self.budget / 4
            {
                break;
            }
        }
        if indices.is_empty() && output_kinds.is_empty() {
            return Ok(None);
        }
        self.retained(&batches)?;
        let mut columns = Vec::<ArrayRef>::with_capacity(self.columns.len());
        for (i, &column) in self.columns.iter().enumerate() {
            if self.partial.is_some() && i == self.columns.len() - 1 {
                columns.push(Arc::new(Int8Array::from(std::mem::take(&mut output_kinds))));
                break;
            }
            let mut arrays: Vec<&dyn Array> =
                batches.iter().map(|b| b.column(column).as_ref()).collect();
            let nulls = new_null_array(self.output.field(i).data_type(), 1);
            let selected = if self.partial.is_some() {
                for index in &mut cell_indices[i] {
                    if index.0 == usize::MAX {
                        index.0 = arrays.len();
                    }
                }
                arrays.push(nulls.as_ref());
                &cell_indices[i]
            } else {
                &indices
            };
            columns.push(interleave(&arrays, selected).map_err(|e| e.to_string())?);
        }
        RecordBatch::try_new(self.output.clone(), columns)
            .map(Some)
            .map_err(|e| e.to_string())
    }

    fn kind(batch: &RecordBatch, keys: usize, row: usize) -> Result<i8> {
        let kinds = batch
            .column(keys + 1)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or("Snapshot kind column is not Int8")?;
        if kinds.is_null(row) {
            return Err("Null snapshot row kind".into());
        }
        match kinds.value(row) {
            kind @ 0..=3 => Ok(kind),
            kind => Err(format!("Invalid Paimon row kind {kind}")),
        }
    }
}
