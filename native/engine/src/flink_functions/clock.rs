use arrow::datatypes::{DataType, TimeUnit};
use chrono::{Datelike, Timelike};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::cell::Cell;

thread_local! {
    static WATERMARK: Cell<i64> = const { Cell::new(i64::MIN) };
}

// Calc evaluation is synchronous on the operator's mailbox thread. Restore the previous context
// on return or unwind so a chained operator or a subsequent task cannot observe another watermark.
pub(crate) fn with_watermark<T>(watermark: i64, evaluate: impl FnOnce() -> T) -> T {
    struct Restore(i64);
    impl Drop for Restore {
        fn drop(&mut self) {
            WATERMARK.set(self.0);
        }
    }
    let _restore = Restore(WATERMARK.replace(watermark));
    evaluate()
}

pub(crate) fn function(field: i32, zone: String) -> ScalarUDF {
    ScalarUDF::new_from_impl(Clock {
        field,
        zone,
        signature: Signature::exact(vec![], Volatility::Volatile),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Clock {
    field: i32,
    zone: String,
    signature: Signature,
}

impl ScalarUDFImpl for Clock {
    fn name(&self) -> &str {
        "flink_clock"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(match self.field {
            0 | 1 | 5 => streamfusion_bridge::timestamp::timestamp_type(),
            2 => DataType::Date32,
            3 => DataType::Time32(TimeUnit::Millisecond),
            4 => DataType::Int64,
            _ => return exec_err!("Unknown clock field {}", self.field),
        })
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let millis = chrono::Utc::now().timestamp_millis();
        let value = match self.field {
            5 => {
                let watermark = WATERMARK.get();
                timestamp_scalar((watermark != i64::MIN).then_some(watermark))
            }
            0 => timestamp_scalar(Some(millis)),
            4 => ScalarValue::Int64(Some(millis.div_euclid(1000))),
            _ => {
                let Some(local) = crate::expr::instant_local(millis, &self.zone) else {
                    return exec_err!("Unsupported clock time zone {}", self.zone);
                };
                match self.field {
                    1 => timestamp_scalar(Some(local.and_utc().timestamp_millis())),
                    2 => ScalarValue::Date32(Some(local.date().num_days_from_ce() - 719_163)),
                    3 => ScalarValue::Time32Millisecond(Some(
                        (local.time().num_seconds_from_midnight() * 1000) as i32,
                    )),
                    _ => return exec_err!("Unknown clock field {}", self.field),
                }
            }
        };
        Ok(ColumnarValue::Scalar(value))
    }
}

fn timestamp_scalar(millis: Option<i64>) -> ScalarValue {
    use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};
    ScalarValue::Struct(std::sync::Arc::new(timestamp_array([
        millis.map(TimestampValue::from_millis)
    ])))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_context_restores_nested_evaluations_and_unwinds() {
        assert_eq!(WATERMARK.get(), i64::MIN);
        with_watermark(123, || {
            assert_eq!(WATERMARK.get(), 123);
            with_watermark(456, || assert_eq!(WATERMARK.get(), 456));
            assert_eq!(WATERMARK.get(), 123);
            let failure = std::panic::catch_unwind(|| {
                with_watermark(789, || panic!("evaluation failed"));
            });
            assert!(failure.is_err());
            assert_eq!(WATERMARK.get(), 123);
        });
        assert_eq!(WATERMARK.get(), i64::MIN);
    }

    #[test]
    fn clocks_and_watermarks_cannot_be_folded_into_plan_time_literals() {
        for field in 0..=5 {
            assert_eq!(
                function(field, "UTC".into()).signature().volatility,
                Volatility::Volatile
            );
        }
    }
}
