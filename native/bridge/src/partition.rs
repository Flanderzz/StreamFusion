use crate::prelude::*;

/// Splits one batch into per-partition sub-batches — Arroyo's Partitioner::partition shape: the
/// partition-key columns row-convert to comparable keys, rows group by key in first-seen order, and
/// one permutation take() reorders every column so each group is a zero-copy slice. Groups keep the
/// full row schema; the JVM reads the partition values off row 0 to route the group to its bucket
/// and the encoder projects the columns out of the written file.
pub fn split_by_partition_columns(
    batch: &RecordBatch,
    partition_columns: &[usize],
) -> Vec<RecordBatch> {
    let key_arrays: Vec<ArrayRef> = partition_columns
        .iter()
        .map(|&index| batch.column(index).clone())
        .collect();
    let converter = RowConverter::new(
        key_arrays
            .iter()
            .map(|array| SortField::new(array.data_type().clone()))
            .collect(),
    )
    .expect("failed to build partition key converter");
    let rows = converter
        .convert_columns(&key_arrays)
        .expect("failed to convert partition keys");

    let mut groups: HashMap<Row, Vec<u32>> = HashMap::default();
    let mut order: Vec<Row> = Vec::new();
    for index in 0..batch.num_rows() {
        let key = rows.row(index);
        groups
            .entry(key)
            .or_insert_with(|| {
                order.push(key);
                Vec::new()
            })
            .push(index as u32);
    }
    // Streaming batches are frequently single-partition (time partitions, pre-shuffled keys), so
    // skip the permutation when there is nothing to reorder.
    if order.len() == 1 {
        return vec![batch.clone()];
    }

    let mut permutation: Vec<u32> = Vec::with_capacity(batch.num_rows());
    let mut lengths: Vec<usize> = Vec::with_capacity(order.len());
    for key in &order {
        let indices = &groups[key];
        permutation.extend_from_slice(indices);
        lengths.push(indices.len());
    }
    let permutation = UInt32Array::from(permutation);
    let permuted_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &permutation, None).expect("failed to permute batch"))
        .collect();
    let permuted =
        RecordBatch::try_new(batch.schema(), permuted_columns).expect("failed to rebuild batch");

    let mut slices = Vec::with_capacity(lengths.len());
    let mut offset = 0;
    for length in lengths {
        slices.push(permuted.slice(offset, length));
        offset += length;
    }
    slices
}
