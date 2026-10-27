use crate::*;

/// Composes a keyed table's two decodes the way Flink's key/value merge does. The input batch is
/// two binary columns `[key, body]`, and each column decodes in one batch: the keys through the
/// key format, the values through the value format, each with its own skip semantics. Flink then
/// emits, per record, every value row paired with every key row, value-major
/// (`OutputProjectionCollector`): a record whose key yields no row (a null JSON key, or a bad one
/// under the key format's `ignore-parse-errors`) emits nothing, while its value is still decoded,
/// and fails, as Flink's is. The key fields fill their physical positions first and the value
/// fields after them, so under `value.fields-include = ALL` the value's copy of a key field wins,
/// exactly `emitRow`'s field order.
pub struct KeyedDecoder {
    pub value: Box<MessageDecoder>,
    pub key: Box<MessageDecoder>,
    pub key_positions: Vec<usize>,
    pub value_positions: Vec<usize>,
    pub output: SchemaRef,
}

impl KeyedDecoder {
    pub fn decode_traced(&self, records: &RecordBatch) -> (RecordBatch, Sources) {
        let messages = records.num_rows();
        let (keys, key_sources) = self.key.decode_traced(
            &records
                .project(&[0])
                .expect("keyed decode expects a two-column [key, body] batch"),
        );
        let (values, value_sources) = self.value.decode_traced(
            &records
                .project(&[1])
                .expect("keyed decode expects a two-column [key, body] batch"),
        );
        let (key_rows, value_rows, sources) = match (&key_sources, &value_sources) {
            (Sources::Identity, Sources::Identity) => (None, None, Sources::Identity),
            // One key row per record: each value row takes its record's.
            (Sources::Identity, Sources::Rows(rows)) => {
                (Some(rows.clone()), None, Sources::Rows(rows.clone()))
            }
            _ => {
                let (key_rows, value_rows, sources) = pair_rows(
                    &key_sources,
                    keys.num_rows(),
                    &value_sources,
                    values.num_rows(),
                    messages,
                );
                (Some(key_rows), Some(value_rows), sources)
            }
        };
        let keys = gather(keys, key_rows);
        let values = gather(values, value_rows);
        let mut columns: Vec<Option<ArrayRef>> = vec![None; self.output.fields().len()];
        for (index, position) in self.key_positions.iter().enumerate() {
            columns[*position] = Some(keys.column(index).clone());
        }
        for (index, position) in self.value_positions.iter().enumerate() {
            columns[*position] = Some(values.column(index).clone());
        }
        let columns = columns
            .into_iter()
            .map(|column| column.expect("keyed decode left a physical column unfilled"))
            .collect();
        let batch = RecordBatch::try_new(self.output.clone(), columns)
            .expect("failed to compose the keyed decode batch");
        (batch, sources)
    }
}

/// Flink's per-record pairing: for each record in order, each of its value rows with each of its
/// key rows, value-major. Returns the key and value rows to gather and each output row's record.
fn pair_rows(
    keys: &Sources,
    key_count: usize,
    values: &Sources,
    value_count: usize,
    messages: usize,
) -> (Vec<u32>, Vec<u32>, Sources) {
    let mut key_rows = Vec::with_capacity(value_count);
    let mut value_rows = Vec::with_capacity(value_count);
    let mut sources = SourceRecorder::with_capacity(value_count);
    let (mut key_start, mut value_start) = (0, 0);
    for message in 0..messages {
        let mut key_end = key_start;
        while key_end < key_count && keys.message(key_end) == message {
            key_end += 1;
        }
        let mut value_end = value_start;
        while value_end < value_count && values.message(value_end) == message {
            value_end += 1;
        }
        for value in value_start..value_end {
            for key in key_start..key_end {
                value_rows.push(value as u32);
                key_rows.push(key as u32);
            }
        }
        sources.message_done(message, value_rows.len());
        (key_start, value_start) = (key_end, value_end);
    }
    (key_rows, value_rows, sources.finish(messages))
}

fn gather(batch: RecordBatch, rows: Option<Vec<u32>>) -> RecordBatch {
    match rows {
        None => batch,
        Some(rows) => arrow::compute::take_record_batch(&batch, &UInt32Array::from(rows))
            .expect("failed to gather keyed decode rows"),
    }
}
