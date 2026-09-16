use arrow::array::{Array, Float64Builder, Int32Builder};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_int32_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::hash::{Hash, Hasher};
use std::sync::{atomic::AtomicU64, atomic::Ordering, Arc, Mutex};

pub(crate) fn function(integer: bool, has_seed: bool, literal_seed: bool) -> ScalarUDF {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    ScalarUDF::new_from_impl(Random {
        id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        integer,
        has_seed,
        literal_seed,
        signature: Signature::exact(
            vec![DataType::Int32; usize::from(integer) + usize::from(has_seed)],
            Volatility::Volatile,
        ),
        state: Mutex::new(None),
    })
}

#[derive(Debug)]
struct Random {
    id: u64,
    integer: bool,
    has_seed: bool,
    literal_seed: bool,
    signature: Signature,
    state: Mutex<Option<JavaRandom>>,
}

// Each call site owns a stream, even when two sites use the same literal seed.
impl PartialEq for Random {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Random {}

impl Hash for Random {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for Random {
    fn name(&self) -> &str {
        "flink_random"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(if self.integer {
            DataType::Int32
        } else {
            DataType::Float64
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = args
            .args
            .iter()
            .map(|value| value.to_array_of_size(args.number_rows))
            .collect::<Result<Vec<_>>>()?;
        let columns = arrays
            .iter()
            .map(|array| as_int32_array(array.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        let mut doubles =
            Float64Builder::with_capacity(if self.integer { 0 } else { args.number_rows });
        let mut integers =
            Int32Builder::with_capacity(if self.integer { args.number_rows } else { 0 });
        let mut state = self.state.lock().unwrap();
        for row in 0..args.number_rows {
            if columns.iter().any(|column| column.is_null(row)) {
                if self.integer {
                    integers.append_null();
                } else {
                    doubles.append_null();
                }
                continue;
            }
            let seed = self.has_seed.then(|| columns[0].value(row) as i64 as u64);
            let mut dynamic;
            let random = if self.has_seed && !self.literal_seed {
                dynamic = JavaRandom::new(seed.unwrap());
                &mut dynamic
            } else {
                state.get_or_insert_with(|| {
                    let seed = seed.unwrap_or_else(|| ahash::RandomState::new().hash_one(self.id));
                    JavaRandom::new(seed)
                })
            };
            if self.integer {
                integers.append_value(random.next_int(columns.last().unwrap().value(row))?);
            } else {
                doubles.append_value(random.next_double());
            }
        }
        Ok(ColumnarValue::Array(if self.integer {
            Arc::new(integers.finish())
        } else {
            Arc::new(doubles.finish())
        }))
    }
}

/// The java.util.Random algorithm used by Flink's RandCallGen for seeded results.
#[derive(Debug)]
struct JavaRandom(u64);

impl JavaRandom {
    const MULTIPLIER: u64 = 0x5deece66d;
    const MASK: u64 = (1 << 48) - 1;

    fn new(seed: u64) -> Self {
        Self((seed ^ Self::MULTIPLIER) & Self::MASK)
    }

    fn next(&mut self, bits: u32) -> u32 {
        self.0 = self.0.wrapping_mul(Self::MULTIPLIER).wrapping_add(11) & Self::MASK;
        (self.0 >> (48 - bits)) as u32
    }

    fn next_double(&mut self) -> f64 {
        let bits = (u64::from(self.next(26)) << 27) + u64::from(self.next(27));
        bits as f64 / (1u64 << 53) as f64
    }

    fn next_int(&mut self, bound: i32) -> Result<i32> {
        if bound <= 0 {
            return exec_err!("RAND_INTEGER bound must be positive");
        }
        if bound & (bound - 1) == 0 {
            return Ok(((bound as u64 * u64::from(self.next(31))) >> 31) as i32);
        }
        loop {
            let bits = self.next(31) as i32;
            let value = bits % bound;
            if bits.wrapping_sub(value).wrapping_add(bound - 1) >= 0 {
                return Ok(value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_java_sequences_are_exact() {
        let mut random = JavaRandom::new(42);
        for expected in [0.7275636800328681, 0.6832234717598454, 0.30871945533265976] {
            assert_eq!(random.next_double(), expected);
        }
        let mut random = JavaRandom::new(42);
        for expected in [30, 63, 48, 84, 70] {
            assert_eq!(random.next_int(100).unwrap(), expected);
        }
        assert!(random.next_int(0).is_err());
        assert!(random.next_int(-1).is_err());
        assert_eq!(random.next_int(1).unwrap(), 0);
    }

    #[test]
    fn calls_are_volatile_and_distinct() {
        let first = function(false, true, true);
        let second = function(false, true, true);
        assert_eq!(first.signature().volatility, Volatility::Volatile);
        assert_ne!(first, second);
    }
}
