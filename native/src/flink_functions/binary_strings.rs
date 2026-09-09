use arrow::array::{Array, ArrayRef, StringArray};
use std::sync::Arc;

pub(super) fn hex_int(args: &[ArrayRef]) -> datafusion::common::Result<ArrayRef> {
    let [arg] = args else {
        return datafusion::common::exec_err!("HEX expects one integer argument");
    };
    let values = datafusion::common::cast::as_int64_array(arg)?;
    let mut output = arrow::array::StringBuilder::with_capacity(values.len(), values.len() * 16);
    for value in values {
        match value {
            Some(value) => {
                // Like Comet's integer HEX, keep at most 16 digits on the stack.
                let mut digits = [0u8; 16];
                let mut start = digits.len();
                let mut remaining = value as u64;
                loop {
                    start -= 1;
                    digits[start] = super::HEX_DIGITS[(remaining & 15) as usize];
                    remaining >>= 4;
                    if remaining == 0 {
                        break;
                    }
                }
                // SAFETY: the written suffix contains only ASCII bytes from HEX_DIGITS.
                output.append_value(unsafe { std::str::from_utf8_unchecked(&digits[start..]) });
            }
            None => output.append_null(),
        }
    }
    Ok(Arc::new(output.finish()))
}

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

pub(super) fn encode(args: &[ArrayRef], base64: bool) -> datafusion::common::Result<ArrayRef> {
    use base64::Engine;

    let [arg] = args else {
        return datafusion::common::exec_err!("string encoding expects one argument");
    };
    let strings = datafusion::common::cast::as_string_array(arg)?;
    let mut offsets = Vec::with_capacity(strings.len() + 1);
    let mut total = 0usize;
    offsets.push(0i32);
    for string in strings {
        let length = string.map_or(Some(0), |s| {
            if base64 {
                base64::encoded_len(s.len(), true)
            } else {
                s.len().checked_mul(2)
            }
        });
        total = length
            .and_then(|len| total.checked_add(len))
            .ok_or_else(|| {
                datafusion::common::exec_datafusion_err!("encoded string array exceeds capacity")
            })?;
        offsets.push(i32::try_from(total).map_err(|_| {
            datafusion::common::exec_datafusion_err!("encoded string array exceeds Utf8 capacity")
        })?);
    }
    // Sizes are known from byte lengths: write directly into the final Arrow values buffer.
    let mut values = vec![0u8; total];
    for (row, string) in strings.iter().enumerate() {
        let Some(string) = string else { continue };
        let output = &mut values[offsets[row] as usize..offsets[row + 1] as usize];
        if base64 {
            base64::engine::general_purpose::STANDARD
                .encode_slice(string.as_bytes(), output)
                .map_err(|e| datafusion::common::exec_datafusion_err!("TO_BASE64: {e}"))?;
        } else {
            for (&byte, pair) in string.as_bytes().iter().zip(output.chunks_exact_mut(2)) {
                pair[0] = super::HEX_DIGITS[(byte >> 4) as usize];
                pair[1] = super::HEX_DIGITS[(byte & 15) as usize];
            }
        }
    }
    Ok(Arc::new(StringArray::new(
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        arrow::buffer::Buffer::from_vec(values),
        strings.nulls().cloned(),
    )))
}
