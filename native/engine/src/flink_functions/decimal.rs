//! Exact decimal expressions use Flink's resolved result type, including overflow-to-NULL.

use arrow::array::{Array, Decimal128Array, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::datatypes::{i256, DataType};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;
use streamfusion_bridge::jdk_decimal::DecimalRescale;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct DecimalCast {
    precision: u8,
    scale: i8,
    signature: Signature,
}

impl DecimalCast {
    pub(crate) fn new(precision: u8, scale: i8) -> Self {
        Self {
            precision,
            scale,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DecimalCast {
    fn name(&self) -> &str {
        "flink_decimal_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [source] = args else {
            return exec_err!("decimal cast requires one argument");
        };
        DecimalRescale::new(source_scale(source)? as i16, self.precision, self.scale)?;
        Ok(DataType::Decimal128(self.precision, self.scale))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [source] = args.args.as_slice() else {
            return exec_err!("decimal cast requires one argument");
        };
        check_lengths(&args.args, args.number_rows)?;
        let rescale = DecimalRescale::new(
            source_scale(&source.data_type())? as i16,
            self.precision,
            self.scale,
        )?;
        let input = DecimalInput::new(source)?;
        if matches!(source, ColumnarValue::Scalar(_)) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                input.value(0).and_then(|value| rescale.narrow(value)),
                self.precision,
                self.scale,
            )));
        }
        let output = (0..args.number_rows)
            .map(|row| input.value(row).and_then(|value| rescale.narrow(value)))
            .collect::<Decimal128Array>()
            .with_precision_and_scale(self.precision, self.scale)?;
        Ok(ColumnarValue::Array(Arc::new(output)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DecimalOp {
    Add,
    Subtract,
    Multiply,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct DecimalBinary {
    op: DecimalOp,
    precision: u8,
    scale: i8,
    signature: Signature,
}

impl DecimalBinary {
    pub(crate) fn new(op: DecimalOp, precision: u8, scale: i8) -> Self {
        Self {
            op,
            precision,
            scale,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DecimalBinary {
    fn name(&self) -> &str {
        match self.op {
            DecimalOp::Add => "flink_decimal_add",
            DecimalOp::Subtract => "flink_decimal_subtract",
            DecimalOp::Multiply => "flink_decimal_multiply",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [left, right] = args else {
            return exec_err!("decimal arithmetic requires two arguments");
        };
        PreparedBinary::new(self, source_scale(left)?, source_scale(right)?)?;
        Ok(DataType::Decimal128(self.precision, self.scale))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, right] = args.args.as_slice() else {
            return exec_err!("decimal arithmetic requires two arguments");
        };
        check_lengths(&args.args, args.number_rows)?;
        let prepared = PreparedBinary::new(
            self,
            source_scale(&left.data_type())?,
            source_scale(&right.data_type())?,
        )?;
        let inputs = (DecimalInput::new(left)?, DecimalInput::new(right)?);
        let evaluate = |row| prepared.evaluate(inputs.0.value(row)?, inputs.1.value(row)?);
        if matches!(
            (left, right),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
        ) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                evaluate(0),
                self.precision,
                self.scale,
            )));
        }
        let output = (0..args.number_rows)
            .map(evaluate)
            .collect::<Decimal128Array>()
            .with_precision_and_scale(self.precision, self.scale)?;
        Ok(ColumnarValue::Array(Arc::new(output)))
    }
}

struct PreparedBinary {
    op: DecimalOp,
    left_factor: i128,
    right_factor: i128,
    rescale: DecimalRescale,
}

impl PreparedBinary {
    fn new(function: &DecimalBinary, left_scale: i8, right_scale: i8) -> Result<Self> {
        let (scale, left_factor, right_factor) = match function.op {
            DecimalOp::Multiply => (left_scale as i16 + right_scale as i16, 1, 1),
            DecimalOp::Add | DecimalOp::Subtract => {
                let scale = left_scale.max(right_scale);
                (
                    scale as i16,
                    10_i128.pow((scale - left_scale) as u32),
                    10_i128.pow((scale - right_scale) as u32),
                )
            }
        };
        Ok(Self {
            op: function.op,
            left_factor,
            right_factor,
            rescale: DecimalRescale::new(scale, function.precision, function.scale)?,
        })
    }

    fn evaluate(&self, left: i128, right: i128) -> Option<i128> {
        let narrow = match self.op {
            DecimalOp::Multiply => left.checked_mul(right),
            DecimalOp::Add | DecimalOp::Subtract => {
                left.checked_mul(self.left_factor).and_then(|left| {
                    let right = right.checked_mul(self.right_factor)?;
                    match self.op {
                        DecimalOp::Add => left.checked_add(right),
                        _ => left.checked_sub(right),
                    }
                })
            }
        };
        if let Some(value) = narrow {
            return self.rescale.narrow(value);
        }
        // Two valid Decimal128 operands need at most 76 digits (77 for aligned addition),
        // bounded by 2 * 10^76, which fits i256. Only the final rounded result must fit (p, s).
        let left = i256::from_i128(left) * i256::from_i128(self.left_factor);
        let right = i256::from_i128(right) * i256::from_i128(self.right_factor);
        let value = match self.op {
            DecimalOp::Add => left + right,
            DecimalOp::Subtract => left - right,
            DecimalOp::Multiply => left * right,
        };
        self.rescale.wide(value)
    }
}

fn check_lengths(args: &[ColumnarValue], rows: usize) -> Result<()> {
    for arg in args {
        if let ColumnarValue::Array(array) = arg {
            if array.len() != rows {
                return exec_err!("decimal operand has {} rows; expected {rows}", array.len());
            }
        }
    }
    Ok(())
}

fn source_scale(source: &DataType) -> Result<i8> {
    match source {
        DataType::Decimal128(p, s) if (1..=38).contains(p) && (0..=*p as i8).contains(s) => Ok(*s),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Null => {
            Ok(0)
        }
        other => exec_err!("unsupported exact decimal operand: {other}"),
    }
}

// Downcast once per batch, and keep literals scalar instead of filling a repeated-value column.
enum DecimalInput<'a> {
    Scalar(Option<i128>),
    Decimal(&'a Decimal128Array),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
}

impl<'a> DecimalInput<'a> {
    fn new(input: &'a ColumnarValue) -> Result<Self> {
        use datafusion::common::cast::as_primitive_array;
        Ok(match input {
            ColumnarValue::Scalar(value) => Self::Scalar(match value {
                ScalarValue::Decimal128(value, _, _) => *value,
                ScalarValue::Int8(value) => value.map(i128::from),
                ScalarValue::Int16(value) => value.map(i128::from),
                ScalarValue::Int32(value) => value.map(i128::from),
                ScalarValue::Int64(value) => value.map(i128::from),
                ScalarValue::Null => None,
                other => return exec_err!("unsupported exact decimal scalar: {other}"),
            }),
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Decimal128(_, _) => Self::Decimal(as_primitive_array(array)?),
                DataType::Int8 => Self::Int8(as_primitive_array(array)?),
                DataType::Int16 => Self::Int16(as_primitive_array(array)?),
                DataType::Int32 => Self::Int32(as_primitive_array(array)?),
                DataType::Int64 => Self::Int64(as_primitive_array(array)?),
                DataType::Null => Self::Scalar(None),
                other => return exec_err!("unsupported exact decimal array: {other}"),
            },
        })
    }

    fn value(&self, row: usize) -> Option<i128> {
        match self {
            Self::Scalar(value) => *value,
            Self::Decimal(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Int8(array) => (!array.is_null(row)).then(|| array.value(row) as i128),
            Self::Int16(array) => (!array.is_null(row)).then(|| array.value(row) as i128),
            Self::Int32(array) => (!array.is_null(row)).then(|| array.value(row) as i128),
            Self::Int64(array) => (!array.is_null(row)).then(|| array.value(row) as i128),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion::common::config::ConfigOptions;

    fn invoke(
        function: &dyn ScalarUDFImpl,
        args: Vec<ColumnarValue>,
        rows: usize,
    ) -> Result<ColumnarValue> {
        let types = args
            .iter()
            .map(ColumnarValue::data_type)
            .collect::<Vec<_>>();
        let output = function.return_type(&types)?;
        function.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new("out", output, true)),
            config_options: Arc::new(ConfigOptions::new()),
        })
    }

    #[test]
    fn cast_preserves_scalar_results_and_nulls() {
        let cast = DecimalCast::new(5, 2);
        for (value, expected) in [
            (Some(999995), None),
            (Some(-999995), None),
            (Some(999994), Some(99999)),
            (Some(-5), Some(-1)),
            (None, None),
        ] {
            let input = ColumnarValue::Scalar(ScalarValue::Decimal128(value, 6, 3));
            let ColumnarValue::Scalar(result) = invoke(&cast, vec![input], 128).unwrap() else {
                panic!("expected scalar");
            };
            assert_eq!(result, ScalarValue::Decimal128(expected, 5, 2));
        }
        let ColumnarValue::Scalar(result) =
            invoke(&cast, vec![ColumnarValue::Scalar(ScalarValue::Null)], 128).unwrap()
        else {
            panic!("expected scalar");
        };
        assert_eq!(result, ScalarValue::Decimal128(None, 5, 2));
    }

    #[test]
    fn cast_validity_and_slices_survive_precision_overflow() {
        let array =
            Decimal128Array::from(vec![Some(0), Some(999995), None, Some(-999994), Some(5)])
                .with_precision_and_scale(6, 3)
                .unwrap()
                .slice(1, 4);
        let input = ColumnarValue::Array(Arc::new(array));
        let ColumnarValue::Array(output) = invoke(&DecimalCast::new(5, 2), vec![input], 4).unwrap()
        else {
            panic!("expected array");
        };
        output.to_data().validate_full().unwrap();
        let expected = Decimal128Array::from(vec![None, None, Some(-99999), Some(1)])
            .with_precision_and_scale(5, 2)
            .unwrap();
        assert_eq!(output.as_ref(), &expected);
        let empty = ColumnarValue::Array(output.slice(0, 0));
        let ColumnarValue::Array(output) = invoke(&DecimalCast::new(5, 2), vec![empty], 0).unwrap()
        else {
            panic!("expected array");
        };
        assert_eq!(output.len(), 0);
        assert_eq!(output.data_type(), &DataType::Decimal128(5, 2));
    }

    #[test]
    fn cast_rejects_bad_arguments_without_panicking() {
        let cast = DecimalCast::new(5, 2);
        assert!(invoke(&cast, vec![], 0).is_err());
        assert!(invoke(
            &cast,
            vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0)))],
            1
        )
        .is_err());
        assert!(invoke(
            &cast,
            vec![ColumnarValue::Array(Arc::new(Int32Array::from(vec![1])))],
            2
        )
        .is_err());
        assert!(DecimalCast::new(39, 0)
            .return_type(&[DataType::Int32])
            .is_err());
    }

    #[test]
    fn binary_arithmetic_matches_big_integer_oracle_across_scales() {
        use num_bigint::BigInt;
        use streamfusion_bridge::jdk_decimal::rescale_half_up;
        let max = 10_i128.pow(38) - 1;
        for op in [DecimalOp::Add, DecimalOp::Subtract, DecimalOp::Multiply] {
            for s1 in [0, 3, 19, 38] {
                for s2 in [0, 3, 19, 38] {
                    for output_scale in [0, 6, 38] {
                        let function = DecimalBinary::new(op, 38, output_scale);
                        let prepared = PreparedBinary::new(&function, s1, s2).unwrap();
                        for a in [-max, -15, 0, 15, max] {
                            for b in [-max, -1, 0, 1, max] {
                                let (raw, scale) = match op {
                                    DecimalOp::Multiply => {
                                        (BigInt::from(a) * BigInt::from(b), s1 as i64 + s2 as i64)
                                    }
                                    _ => {
                                        let scale = s1.max(s2);
                                        let left = BigInt::from(a)
                                            * BigInt::from(10).pow((scale - s1) as u32);
                                        let right = BigInt::from(b)
                                            * BigInt::from(10).pow((scale - s2) as u32);
                                        (
                                            if op == DecimalOp::Add {
                                                left + right
                                            } else {
                                                left - right
                                            },
                                            scale as i64,
                                        )
                                    }
                                };
                                assert_eq!(
                                    prepared.evaluate(a, b),
                                    rescale_half_up(raw, scale, 38, output_scale),
                                    "{op:?}: ({a}, {s1}), ({b}, {s2}) -> (38, {output_scale})"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn binary_preserves_scalar_broadcast_and_sliced_validity() {
        let max = 10_i128.pow(38) - 1;
        let array = Decimal128Array::from(vec![Some(17), Some(max), None, Some(-1), Some(0)])
            .with_precision_and_scale(38, 0)
            .unwrap()
            .slice(1, 4);
        let scalar = ColumnarValue::Scalar(ScalarValue::Decimal128(Some(1), 38, 0));
        let function = DecimalBinary::new(DecimalOp::Add, 38, 0);
        for args in [
            vec![
                ColumnarValue::Array(Arc::new(array.clone())),
                scalar.clone(),
            ],
            vec![
                scalar.clone(),
                ColumnarValue::Array(Arc::new(array.clone())),
            ],
        ] {
            let ColumnarValue::Array(output) = invoke(&function, args, 4).unwrap() else {
                panic!("expected array");
            };
            output.to_data().validate_full().unwrap();
            let expected = Decimal128Array::from(vec![None, None, Some(0), Some(1)])
                .with_precision_and_scale(38, 0)
                .unwrap();
            assert_eq!(output.as_ref(), &expected);
        }
        let ColumnarValue::Scalar(output) =
            invoke(&function, vec![scalar.clone(), scalar.clone()], 100).unwrap()
        else {
            panic!("expected scalar");
        };
        assert_eq!(output, ScalarValue::Decimal128(Some(2), 38, 0));
        let ColumnarValue::Array(output) = invoke(
            &function,
            vec![scalar, ColumnarValue::Array(Arc::new(array.slice(0, 0)))],
            0,
        )
        .unwrap() else {
            panic!("expected empty array");
        };
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn binary_rejects_bad_arguments_without_panicking() {
        let function = DecimalBinary::new(DecimalOp::Multiply, 38, 6);
        let scalar = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        assert!(invoke(&function, vec![scalar.clone()], 0).is_err());
        assert!(invoke(
            &function,
            vec![
                scalar.clone(),
                ColumnarValue::Scalar(ScalarValue::Float32(Some(1.0)))
            ],
            1
        )
        .is_err());
        assert!(invoke(
            &function,
            vec![
                scalar,
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![1])))
            ],
            2
        )
        .is_err());
    }
}
