use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;
use streamfusion_bridge::timestamp::{
    timestamp_array, timestamp_type, TimestampColumn, TimestampValue,
};

pub(crate) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(TemporalRound {
        signature: Signature::any(3, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TemporalRound {
    signature: Signature,
}

impl ScalarUDFImpl for TemporalRound {
    fn name(&self) -> &str {
        "flink_temporal_round"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(timestamp_type())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(round, vec![])(&args.args)
    }
}

fn round(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let timestamps = TimestampColumn::try_new(arrays[0].as_ref())?;
    let widths = arrays[1].as_any().downcast_ref::<Int64Array>().unwrap();
    let ceilings = arrays[2].as_any().downcast_ref::<BooleanArray>().unwrap();
    let mut result = Vec::with_capacity(timestamps.len());
    for row in 0..timestamps.len() {
        if timestamps.is_null(row) || widths.is_null(row) || ceilings.is_null(row) {
            result.push(None);
            continue;
        }
        let value = timestamps.value(row)?;
        let width = widths.value(row);
        if width <= 0 {
            return exec_err!("Temporal rounding width must be positive");
        }
        let millis = value.millis();
        let ceiling = ceilings.value(row);
        let rounded = if width == 1 {
            millis.wrapping_add(i64::from(ceiling && value.nano_of_milli() != 0))
        } else {
            // SqlFunctionUtils.ceil(long, long) adds width - 1 before floor, with Java
            // overflow. Fractions are ignored except when rounding to MILLISECOND itself.
            let value = if ceiling {
                millis.wrapping_add(width - 1)
            } else {
                millis
            };
            value.wrapping_sub(value.rem_euclid(width))
        };
        result.push(Some(TimestampValue::from_millis(rounded)));
    }
    Ok(Arc::new(timestamp_array(result)))
}
