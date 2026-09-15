use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

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
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(
            |arrays: &[ArrayRef]| {
                let timestamps = arrow::compute::cast(
                    &arrays[0],
                    &DataType::Timestamp(TimeUnit::Nanosecond, None),
                )?;
                round(&[timestamps, arrays[1].clone(), arrays[2].clone()])
            },
            vec![],
        )(&args.args)
    }
}

fn round(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let units_per_milli = 1_000_000;
    let timestamps = arrays[0]
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let widths = arrays[1].as_any().downcast_ref::<Int64Array>().unwrap();
    let ceilings = arrays[2].as_any().downcast_ref::<BooleanArray>().unwrap();
    let mut result = TimestampNanosecondArray::builder(timestamps.len());
    for row in 0..timestamps.len() {
        if timestamps.is_null(row) || widths.is_null(row) || ceilings.is_null(row) {
            result.append_null();
            continue;
        }
        let value = timestamps.value(row);
        let width = widths.value(row);
        if width <= 0 {
            return exec_err!("Temporal rounding width must be positive");
        }
        let millis = value.div_euclid(units_per_milli);
        let ceiling = ceilings.value(row);
        let rounded = if width == 1 {
            millis + i64::from(ceiling && value.rem_euclid(units_per_milli) != 0)
        } else {
            // Flink rounds getMillisecond(), ignoring sub-millisecond digits except
            // when the requested unit itself is MILLISECOND.
            let floor = millis.div_euclid(width) * width;
            floor + if ceiling && millis != floor { width } else { 0 }
        };
        let Some(value) = rounded.checked_mul(units_per_milli) else {
            return exec_err!(
                "Rounded timestamp exceeds the range of {:?}",
                timestamps.data_type()
            );
        };
        result.append_value(value);
    }
    Ok(Arc::new(
        result
            .finish()
            .with_data_type(timestamps.data_type().clone()),
    ))
}
