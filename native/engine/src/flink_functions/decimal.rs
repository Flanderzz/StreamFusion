//! Exact decimal expressions use Flink's resolved result type, including overflow-to-NULL.

use arrow::array::{Array, Decimal128Array, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::datatypes::DataType;
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
}
