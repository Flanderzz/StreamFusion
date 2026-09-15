use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use datafusion::common::{cast::as_primitive_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(FlinkSign {
        signature: Signature::uniform(
            1,
            vec![DataType::Float64, DataType::Float32],
            Volatility::Immutable,
        ),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FlinkSign {
    signature: Signature,
}

impl ScalarUDFImpl for FlinkSign {
    fn name(&self) -> &str {
        "flink_sign"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        Ok(types[0].clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(sign, vec![])(&args.args)
    }
}

fn sign(args: &[ArrayRef]) -> Result<ArrayRef> {
    macro_rules! sign {
        ($t:ty) => {
            Arc::new(as_primitive_array::<$t>(&args[0])?.unary::<_, $t>(|value| {
                if value == 0.0 || value.is_nan() {
                    value
                } else {
                    value.signum()
                }
            }))
        };
    }
    Ok(match args[0].data_type() {
        DataType::Float32 => sign!(Float32Type),
        DataType::Float64 => sign!(Float64Type),
        other => return exec_err!("SIGN expected floating input, got {other}"),
    })
}
