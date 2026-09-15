//! Flink timestamps retain milliseconds separately from the sub-millisecond remainder.
//! Reading a key or event time must not narrow that range to an i64 nanosecond count.

use arrow::array::{
    Array, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::error::ArrowError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimestampValue {
    millis: i64,
    nano_of_milli: u32,
}

impl TimestampValue {
    pub fn millis(self) -> i64 {
        self.millis
    }

    pub fn nano_of_milli(self) -> u32 {
        self.nano_of_milli
    }

    pub fn from_arrow(value: i64, unit: TimeUnit) -> Result<Self, ArrowError> {
        let (millis, nano_of_milli) = match unit {
            TimeUnit::Second => (
                value.checked_mul(1000).ok_or_else(|| {
                    ArrowError::ComputeError(
                        "Timestamp seconds exceed Flink's millisecond range".into(),
                    )
                })?,
                0,
            ),
            TimeUnit::Millisecond => (value, 0),
            TimeUnit::Microsecond => (value.div_euclid(1000), value.rem_euclid(1000) as u32 * 1000),
            TimeUnit::Nanosecond => (
                value.div_euclid(1_000_000),
                value.rem_euclid(1_000_000) as u32,
            ),
        };
        Ok(Self {
            millis,
            nano_of_milli,
        })
    }
}

/// A borrowed view; constructing it neither copies buffers nor takes ownership of the array.
pub struct TimestampColumn<'a> {
    values: &'a ScalarBuffer<i64>,
    nulls: Option<&'a NullBuffer>,
    unit: TimeUnit,
}

impl<'a> TimestampColumn<'a> {
    pub fn try_new(array: &'a dyn Array) -> Result<Self, ArrowError> {
        let DataType::Timestamp(unit, _) = array.data_type() else {
            return Err(ArrowError::CastError(format!(
                "Expected TIMESTAMP, got {}",
                array.data_type()
            )));
        };
        macro_rules! values {
            ($type:ty) => {
                array
                    .as_any()
                    .downcast_ref::<$type>()
                    .ok_or_else(|| {
                        ArrowError::CastError(
                            "Timestamp array does not match its declared unit".into(),
                        )
                    })?
                    .values()
            };
        }
        let values = match unit {
            TimeUnit::Second => values!(TimestampSecondArray),
            TimeUnit::Millisecond => values!(TimestampMillisecondArray),
            TimeUnit::Microsecond => values!(TimestampMicrosecondArray),
            TimeUnit::Nanosecond => values!(TimestampNanosecondArray),
        };
        Ok(Self {
            values,
            nulls: array.nulls(),
            unit: *unit,
        })
    }

    /// The caller checks validity before reading a row, as with Arrow's primitive accessors.
    pub fn value(&self, row: usize) -> Result<TimestampValue, ArrowError> {
        TimestampValue::from_arrow(self.values[row], self.unit)
    }

    pub fn to_millis(&self) -> Result<Int64Array, ArrowError> {
        if self.unit == TimeUnit::Millisecond {
            return Ok(Int64Array::new(self.values.clone(), self.nulls.cloned()));
        }
        (0..self.values.len())
            .map(|row| {
                if self.nulls.is_some_and(|nulls| nulls.is_null(row)) {
                    Ok(None)
                } else {
                    self.value(row).map(|value| Some(value.millis()))
                }
            })
            .collect()
    }

    pub fn max_millis(&self) -> Result<Option<i64>, ArrowError> {
        // Unit conversion is monotonic. Find the maximum before converting, without an output array.
        self.values
            .iter()
            .enumerate()
            .filter(|(row, _)| self.nulls.is_none_or(|nulls| nulls.is_valid(*row)))
            .map(|(_, value)| *value)
            .max()
            .map(|raw| TimestampValue::from_arrow(raw, self.unit).map(TimestampValue::millis))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negative_fractions_keep_a_nonnegative_remainder() {
        for (raw, unit, millis, nanos) in [
            (-1, TimeUnit::Second, -1000, 0),
            (-1, TimeUnit::Millisecond, -1, 0),
            (-1, TimeUnit::Microsecond, -1, 999_000),
            (-1, TimeUnit::Nanosecond, -1, 999_999),
            (-1_000_001, TimeUnit::Nanosecond, -2, 999_999),
            (1_000_001, TimeUnit::Nanosecond, 1, 1),
        ] {
            let value = TimestampValue::from_arrow(raw, unit).unwrap();
            assert_eq!((value.millis(), value.nano_of_milli()), (millis, nanos));
        }
    }

    #[test]
    fn millisecond_range_does_not_depend_on_nanosecond_range() {
        for millis in [
            i64::MIN,
            -62_135_596_800_000,
            253_402_300_799_999,
            31_494_784_780_800_000,
            i64::MAX,
        ] {
            let value = TimestampValue::from_arrow(millis, TimeUnit::Millisecond).unwrap();
            assert_eq!(value.millis(), millis);
            assert_eq!(value.nano_of_milli(), 0);
        }
        for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
            let scale = if unit == TimeUnit::Microsecond {
                1000
            } else {
                1_000_000
            };
            for raw in [i64::MIN, i64::MAX] {
                let value = TimestampValue::from_arrow(raw, unit).unwrap();
                let nanos =
                    i128::from(value.millis()) * 1_000_000 + i128::from(value.nano_of_milli());
                assert_eq!(nanos, i128::from(raw) * (1_000_000 / scale));
            }
        }
        assert!(TimestampValue::from_arrow(i64::MAX, TimeUnit::Second).is_err());
        assert!(TimestampValue::from_arrow(i64::MIN, TimeUnit::Second).is_err());
    }

    #[test]
    fn column_reads_slices_nulls_and_timezone_labels() {
        let array = TimestampNanosecondArray::from(vec![
            Some(99),
            None,
            Some(i64::MIN),
            Some(-1),
            Some(999),
        ])
        .with_timezone("Asia/Shanghai")
        .slice(1, 3);
        let column = TimestampColumn::try_new(&array).unwrap();
        assert_eq!(column.max_millis().unwrap(), Some(-1));
        assert_eq!(
            column.to_millis().unwrap(),
            Int64Array::from(vec![None, Some(-9_223_372_036_855), Some(-1)])
        );
        let minimum = array.slice(1, 1);
        assert_eq!(
            TimestampColumn::try_new(&minimum)
                .unwrap()
                .max_millis()
                .unwrap(),
            Some(-9_223_372_036_855)
        );
        let nulls = TimestampSecondArray::new_null(3);
        let column = TimestampColumn::try_new(&nulls).unwrap();
        assert_eq!(column.max_millis().unwrap(), None);
        assert_eq!(column.to_millis().unwrap().null_count(), 3);
        let empty = nulls.slice(0, 0);
        assert_eq!(
            TimestampColumn::try_new(&empty)
                .unwrap()
                .max_millis()
                .unwrap(),
            None
        );
    }

    #[test]
    fn milliseconds_share_buffers() {
        let array =
            TimestampMillisecondArray::from(vec![Some(i64::MIN), None, Some(i64::MAX)]).slice(1, 2);
        let output = TimestampColumn::try_new(&array)
            .unwrap()
            .to_millis()
            .unwrap();
        assert_eq!(array.values().as_ptr(), output.values().as_ptr());
        assert_eq!(output, Int64Array::from(vec![None, Some(i64::MAX)]));
    }
}
