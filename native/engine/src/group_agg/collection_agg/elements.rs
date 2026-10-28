use super::super::numeric_map_order::FloatingKey;
use super::*;

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum FloatElement {
    Float32,
    Float64,
}

impl CollectionElement for FloatElement {
    type Value = FloatingKey;
    fn from_type(ty: &DataType) -> Self {
        match ty {
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            _ => unreachable!("floating collection element"),
        }
    }
    fn scalar(&self, value: Option<FloatingKey>) -> ScalarValue {
        match self {
            Self::Float32 => ScalarValue::Float32(value.map(|key| f32::from_bits(key.bits as u32))),
            Self::Float64 => ScalarValue::Float64(value.map(|key| f64::from_bits(key.bits))),
        }
    }
    fn value(&self, value: &ScalarValue) -> Option<FloatingKey> {
        match (self, value) {
            (Self::Float32, ScalarValue::Float32(value)) => value.map(FloatingKey::from_f32),
            (Self::Float64, ScalarValue::Float64(value)) => value.map(FloatingKey::from_f64),
            _ => unreachable!("floating collection scalar"),
        }
    }
    fn hash(&self, value: FloatingKey) -> i32 {
        let canonical = value.value.canonical();
        let hash = match self {
            Self::Float32 => (canonical as f32).to_bits(),
            Self::Float64 => {
                let bits = canonical.to_bits();
                (bits ^ (bits >> 32)) as u32
            }
        };
        (hash ^ (hash >> 16)) as i32
    }
    fn array(&self, values: impl IntoIterator<Item = Option<FloatingKey>>) -> ArrayRef {
        match self {
            Self::Float32 => Arc::new(arrow::array::Float32Array::from(
                values
                    .into_iter()
                    .map(|key| key.map(|key| f32::from_bits(key.bits as u32)))
                    .collect::<Vec<_>>(),
            )),
            Self::Float64 => Arc::new(arrow::array::Float64Array::from(
                values
                    .into_iter()
                    .map(|key| key.map(|key| f64::from_bits(key.bits)))
                    .collect::<Vec<_>>(),
            )),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct DecimalElement {
    precision: u8,
    scale: i8,
}

impl CollectionElement for DecimalElement {
    type Value = i128;
    fn from_type(ty: &DataType) -> Self {
        let DataType::Decimal128(precision, scale) = ty else {
            unreachable!("decimal collection element")
        };
        Self {
            precision: *precision,
            scale: *scale,
        }
    }
    fn scalar(&self, value: Option<i128>) -> ScalarValue {
        ScalarValue::Decimal128(value, self.precision, self.scale)
    }
    fn value(&self, value: &ScalarValue) -> Option<i128> {
        let ScalarValue::Decimal128(value, _, _) = value else {
            unreachable!("decimal collection scalar")
        };
        *value
    }
    fn hash(&self, value: i128) -> i32 {
        super::super::decimal_map_order::decimal_hash(value, self.scale)
    }
    fn array(&self, values: impl IntoIterator<Item = Option<i128>>) -> ArrayRef {
        Arc::new(
            arrow::array::Decimal128Array::from(values.into_iter().collect::<Vec<_>>())
                .with_precision_and_scale(self.precision, self.scale)
                .expect("verified decimal element type"),
        )
    }
}
