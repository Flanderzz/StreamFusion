use arrow::array::{Array, ArrayRef, Date32Array, Time32MillisecondArray};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;
use streamfusion_bridge::timestamp::{
    is_timestamp, timestamp_array, timestamp_type, TimestampColumn,
};

pub(super) fn function(arity: usize, greatest: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(TemporalExtremum {
        greatest,
        signature: Signature::any(arity, Volatility::Immutable),
    })
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct TemporalExtremum {
    greatest: bool,
    signature: Signature,
}
impl ScalarUDFImpl for TemporalExtremum {
    fn name(&self) -> &str {
        if self.greatest {
            "flink_temporal_greatest"
        } else {
            "flink_temporal_least"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if types.len() < 2 || types.iter().any(|ty| ty != &types[0]) {
            return exec_err!("Temporal extrema require matching physical types");
        }
        Ok(if is_timestamp(&types[0]) {
            timestamp_type()
        } else {
            types[0].clone()
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(
            |arrays: &[ArrayRef]| {
                let rows = arrays[0].len();
                macro_rules! primitive {
                    ($ty:ty) => {{
                        let values = arrays
                            .iter()
                            .map(|a| a.as_any().downcast_ref::<$ty>().unwrap())
                            .collect::<Vec<_>>();
                        let result = (0..rows).map(|row| {
                            if values.iter().any(|a| a.is_null(row)) {
                                return None;
                            }
                            let values = values.iter().map(|a| a.value(row));
                            if self.greatest {
                                values.max()
                            } else {
                                values.min()
                            }
                        });
                        Arc::new(<$ty>::from_iter(result)) as ArrayRef
                    }};
                }
                Ok(match arrays[0].data_type() {
                    DataType::Date32 => primitive!(Date32Array),
                    DataType::Time32(_) => primitive!(Time32MillisecondArray),
                    ty if is_timestamp(ty) => {
                        let values = arrays
                            .iter()
                            .map(|a| {
                                TimestampColumn::try_new(a.as_ref())
                                    .map_err(datafusion::common::DataFusionError::from)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let result = (0..rows)
                            .map(|row| {
                                if values.iter().any(|a| a.is_null(row)) {
                                    return Ok(None);
                                }
                                let mut selected = values[0].value(row)?;
                                for value in &values[1..] {
                                    let value = value.value(row)?;
                                    let greater = (value.millis(), value.nano_of_milli())
                                        > (selected.millis(), selected.nano_of_milli());
                                    if greater == self.greatest {
                                        selected = value;
                                    }
                                }
                                Ok(Some(selected))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Arc::new(timestamp_array(result))
                    }
                    ty => return exec_err!("Unexpected temporal extremum type {ty}"),
                })
            },
            vec![],
        )(&args.args)
    }
}
