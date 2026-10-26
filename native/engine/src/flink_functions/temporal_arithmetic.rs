use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Int32Array, Int64Array, Time32MillisecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;
use streamfusion_bridge::timestamp::{
    timestamp_array, timestamp_type, TimestampColumn, TimestampValue,
};

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub(super) enum Operation {
    TimestampMillis,
    DateMillis,
    TimeMillis,
    DateMonths,
    TimestampMonths,
}

pub(super) fn function(operation: Operation) -> ScalarUDF {
    ScalarUDF::new_from_impl(TemporalArithmetic {
        operation,
        signature: Signature::any(3, Volatility::Immutable),
    })
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct TemporalArithmetic {
    operation: Operation,
    signature: Signature,
}
impl ScalarUDFImpl for TemporalArithmetic {
    fn name(&self) -> &str {
        "flink_temporal_arithmetic"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(match self.operation {
            Operation::DateMillis | Operation::DateMonths => DataType::Date32,
            Operation::TimeMillis => DataType::Time32(TimeUnit::Millisecond),
            _ => timestamp_type(),
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(
            |arrays: &[ArrayRef]| {
                let dates = arrays[0].as_any().downcast_ref::<Date32Array>();
                let times = arrays[0].as_any().downcast_ref::<Time32MillisecondArray>();
                let timestamps = if dates.is_none() && times.is_none() {
                    Some(TimestampColumn::try_new(arrays[0].as_ref())?)
                } else {
                    None
                };
                let months = arrays[1].as_any().downcast_ref::<Int32Array>();
                let millis = arrays[1].as_any().downcast_ref::<Int64Array>();
                let subtract = arrays[2].as_any().downcast_ref::<BooleanArray>().unwrap();
                let mut output = Vec::with_capacity(arrays[0].len());
                for row in 0..arrays[0].len() {
                    if arrays.iter().any(|a| a.is_null(row)) {
                        output.push(None);
                        continue;
                    }
                    let value = if let Some(dates) = dates {
                        TimestampValue::from_millis(i64::from(dates.value(row)) * 86_400_000)
                    } else if let Some(times) = times {
                        TimestampValue::from_millis(i64::from(times.value(row)))
                    } else {
                        timestamps.as_ref().unwrap().value(row)?
                    };
                    let delta = if let Some(months) = months {
                        i64::from(if subtract.value(row) {
                            months.value(row).wrapping_neg()
                        } else {
                            months.value(row)
                        })
                    } else if let Some(millis) = millis {
                        let value = if self.operation == Operation::TimeMillis {
                            millis.value(row) % 86_400_000
                        } else {
                            millis.value(row)
                        };
                        if subtract.value(row) {
                            value.wrapping_neg()
                        } else {
                            value
                        }
                    } else {
                        return exec_err!("Temporal interval has an unexpected physical type");
                    };
                    let result = match self.operation {
                        Operation::TimestampMonths | Operation::DateMonths => {
                            super::calendar::add_months(value.millis(), delta as i32)
                        }
                        Operation::TimeMillis => {
                            (value.millis() + 86_400_000 + delta % 86_400_000) % 86_400_000
                        }
                        Operation::DateMillis => {
                            let delta = i32::try_from(delta / 86_400_000).map_err(|_| {
                                datafusion::common::DataFusionError::Execution(
                                    "DATE interval day count overflows INT".into(),
                                )
                            })?;
                            i64::from(dates.unwrap().value(row).wrapping_add(delta)) * 86_400_000
                        }
                        Operation::TimestampMillis => value.millis().wrapping_add(delta),
                    };
                    output.push(Some(TimestampValue::new(result, value.nano_of_milli())?));
                }
                Ok(match self.operation {
                    Operation::DateMillis | Operation::DateMonths => {
                        Arc::new(Date32Array::from_iter(
                            output
                                .into_iter()
                                .map(|v| v.map(|v| (v.millis() / 86_400_000) as i32)),
                        )) as ArrayRef
                    }
                    Operation::TimeMillis => Arc::new(Time32MillisecondArray::from_iter(
                        output.into_iter().map(|v| v.map(|v| v.millis() as i32)),
                    )) as ArrayRef,
                    _ => Arc::new(timestamp_array(output)) as ArrayRef,
                })
            },
            vec![],
        )(&args.args)
    }
}
