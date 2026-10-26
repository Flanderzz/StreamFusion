use arrow::array::{Array, ArrayRef, BooleanArray, Date32Array};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;
use streamfusion_bridge::timestamp::{
    is_timestamp, timestamp_array, timestamp_type, TimestampColumn, TimestampValue,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(CalendarRound {
        signature: Signature::any(3, Volatility::Immutable),
    })
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct CalendarRound {
    signature: Signature,
}
impl ScalarUDFImpl for CalendarRound {
    fn name(&self) -> &str {
        "flink_calendar_round"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types.first() {
            Some(DataType::Date32) => Ok(DataType::Date32),
            Some(t) if is_timestamp(t) => Ok(timestamp_type()),
            _ => exec_err!("Calendar rounding requires DATE or plain TIMESTAMP"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(round, vec![])(&args.args)
    }
}
fn round(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let years = arrays[1].as_any().downcast_ref::<BooleanArray>().unwrap();
    let ceilings = arrays[2].as_any().downcast_ref::<BooleanArray>().unwrap();
    let dates = arrays[0].as_any().downcast_ref::<Date32Array>();
    let timestamps = if dates.is_none() {
        Some(TimestampColumn::try_new(arrays[0].as_ref())?)
    } else {
        None
    };
    let result = (0..arrays[0].len())
        .map(|row| {
            if arrays.iter().any(|a| a.is_null(row)) {
                return Ok(None);
            }
            let days = if let Some(dates) = dates {
                dates.value(row)
            } else {
                (timestamps.as_ref().unwrap().value(row)?.millis() / 86_400_000) as i32
            };
            Ok(Some(super::calendar::round_date(
                days,
                years.value(row),
                ceilings.value(row),
            )))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(if dates.is_some() {
        Arc::new(Date32Array::from(result))
    } else {
        Arc::new(timestamp_array(result.into_iter().map(|d| {
            d.map(|d| TimestampValue::from_millis(i64::from(d) * 86_400_000))
        })))
    })
}
