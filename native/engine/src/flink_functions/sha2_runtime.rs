use arrow::array::{Array, ArrayRef, Int32Array, NullBufferBuilder, StringArray};
use arrow::buffer::OffsetBuffer;
use std::sync::Arc;

/// Invalid widths retain the released evaluator and its JDK exception.
pub(crate) fn evaluate(args: [&ArrayRef; 2]) -> Option<ArrayRef> {
    let strings = args[0].as_any().downcast_ref::<StringArray>()?;
    let bits = args[1].as_any().downcast_ref::<Int32Array>()?;
    let mut capacity = 0usize;
    for (value, bits) in strings.iter().zip(bits.iter()) {
        if value.is_some() {
            if let Some(bits) = bits {
                if !matches!(bits, 224 | 256 | 384 | 512) {
                    return None;
                }
                capacity = capacity.checked_add(bits as usize / 4)?;
            }
        }
    }
    if capacity > i32::MAX as usize {
        return None;
    }
    let mut values = Vec::with_capacity(capacity);
    let mut offsets = Vec::with_capacity(strings.len() + 1);
    let mut nulls = NullBufferBuilder::new(strings.len());
    offsets.push(0i32);
    for (value, bits) in strings.iter().zip(bits.iter()) {
        if let (Some(value), Some(bits)) = (value, bits) {
            use crate::expr::append_digest_hex;
            match bits {
                224 => append_digest_hex::<sha2::Sha224>(value, &mut values),
                256 => append_digest_hex::<sha2::Sha256>(value, &mut values),
                384 => append_digest_hex::<sha2::Sha384>(value, &mut values),
                512 => append_digest_hex::<sha2::Sha512>(value, &mut values),
                _ => unreachable!("widths verified before hashing"),
            }
            nulls.append_non_null();
        } else {
            nulls.append_null();
        }
        offsets.push(values.len() as i32);
    }
    // SAFETY: the shared digest helper writes only ASCII hex; verified widths bound the total
    // by i32::MAX, each offset delimits a completed digest, and the NULL mask has one bit per row.
    Some(Arc::new(unsafe {
        StringArray::new_unchecked(
            OffsetBuffer::new(offsets.into()),
            values.into(),
            nulls.finish(),
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_widths_match_known_digests_on_sliced_and_empty_inputs() {
        let source = Arc::new(StringArray::from(vec![
            Some("ignored"),
            Some(""),
            Some("a\0b"),
            Some("\u{1f600}\u{4e2d}"),
            Some("abc"),
            None,
            Some("abc"),
            None,
        ])) as ArrayRef;
        let bits = Arc::new(Int32Array::from(vec![
            Some(0),
            Some(224),
            Some(256),
            Some(384),
            Some(512),
            Some(128),
            None,
            None,
        ])) as ArrayRef;
        let args = [source.slice(1, 7), bits.slice(1, 7)];
        let result = evaluate(args.each_ref()).unwrap();
        let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.value(0),
            "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f"
        );
        assert_eq!(
            strings.value(1),
            "59b271ae1bbcb1d31d41929817f4b16fb439eb4f31520b5ad1d5ce98920a7138"
        );
        assert_eq!(
            strings.value(2),
            concat!(
                "264d790aef28f80f0541d219ecb440cbc6a2c4d5b555543586f9c025da9c89c2f",
                "9b440017034886be9e9dfe03373d815"
            )
        );
        assert_eq!(
            strings.value(3),
            concat!(
                "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a",
                "2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
            )
        );
        assert!((4..7).all(|row| strings.is_null(row)));
        result.to_data().validate_full().unwrap();
        let empty = args.map(|arg| arg.slice(0, 0));
        assert_eq!(evaluate(empty.each_ref()).unwrap().len(), 0);
    }

    #[test]
    fn every_non_null_invalid_width_retains_java_even_after_valid_rows() {
        for invalid in [i32::MIN, -1, 0, 128, 225, 257, 513, i32::MAX] {
            let source = Arc::new(StringArray::from(vec!["abc", "abc"])) as ArrayRef;
            let bits = Arc::new(Int32Array::from(vec![256, invalid])) as ArrayRef;
            assert!(evaluate([&source, &bits]).is_none());
        }
    }

    #[test]
    fn compiled_dynamic_widths_do_not_invoke_the_jvm_or_use_decoded_caches() {
        use crate::calc::CalcExpression;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use streamfusion_bridge::string_cache::{string_cache_array, string_cache_field};

        let source = Arc::new(StringArray::from(vec!["?", "abc", "\u{1f600}", ""])) as ArrayRef;
        let bits = Arc::new(Int32Array::from(vec![224, 256, 384, 512])) as ArrayRef;
        let expected = evaluate([&source, &bits]).unwrap();
        let cache = string_cache_array([Some([0xd800].as_slice()), None, None, None]);
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("s", DataType::Utf8, true),
                Field::new("bits", DataType::Int32, true),
                string_cache_field(0, cache.data_type().clone()),
            ])),
            vec![source, bits, cache],
        )
        .unwrap();
        let mut calc = CalcExpression {
            kinds: vec![44, 0, 0],
            payload: vec![0, 0, 1],
            child_counts: vec![2, 0, 0],
            longs: vec![-1, 0, 12],
            doubles: vec![],
            strings: vec![],
            projection_roots: vec![0],
            condition_root: -1,
            output_names: vec!["v".into()],
            compiled: None,
        };
        for _ in 0..2 {
            assert_eq!(calc.evaluate(input.clone()).column(0), &expected);
            assert_eq!(
                calc.evaluate(input.slice(1, 2)).column(0),
                &expected.slice(1, 2)
            );
        }
        assert_eq!(calc.evaluate(input.slice(0, 0)).num_rows(), 0);
    }
}
