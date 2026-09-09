use arrow::array::{Array, ArrayRef, StringArray};
use std::sync::Arc;

pub(super) fn bin(args: &[ArrayRef]) -> datafusion::common::Result<ArrayRef> {
    use std::fmt::Write;

    let [arg] = args else {
        return datafusion::common::exec_err!("BIN expects one argument");
    };
    let values = datafusion::common::cast::as_int64_array(arg)?;
    let mut builder = arrow::array::StringBuilder::with_capacity(values.len(), values.len() * 64);
    for value in values {
        match value {
            Some(value) => {
                // Flink uses Long.toBinaryString, including 64-bit two's complement negatives.
                write!(&mut builder, "{:b}", value as u64)
                    .map_err(|e| datafusion::common::exec_datafusion_err!("BIN: {e}"))?;
                builder.append_value("");
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}
