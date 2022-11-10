use super::*;
use crate::array::ArrayImpl;
use crate::types::F64;
use crate::v1::binder::BindError;

pub struct AbsFunction {
    return_type: DataType,
}

impl AbsFunction {
    // TODO: bind scalar function
    #![allow(dead_code)]
    pub fn try_create(input_types: &[DataType]) -> Result<Box<dyn Function>, BindError> {
        if input_types.len() != 1 {
            return Err(BindError::BindFunctionError(
                "The column size should be 1".to_string(),
            ));
        }

        Ok(Box::new(AbsFunction {
            return_type: input_types[0].clone(),
        }))
    }
}

impl Function for AbsFunction {
    fn name(&self) -> &str {
        "abs"
    }

    fn return_types(&self) -> DataType {
        // TODO: When unsigned types are supported,
        // we can convert signed type to unsigned type
        // this makes abs(i32::MIN) can represent by u32
        self.return_type.clone()
    }

    fn execute(&self, input: &[&ArrayImpl]) -> Result<ArrayImpl, FunctionError> {
        let arr = input[0];
        match &arr {
            ArrayImpl::Int32(_) => {
                let f = |x: &i32, _: &mut FunctionCtx| x.abs();
                let res_arr =
                    UnaryExecutor::eval_batch_lazy_select::<I32Array, I32Array, _>(arr, f)?;
                Ok(res_arr)
            }
            ArrayImpl::Int64(_) => {
                let f = |x: &i64, _: &mut FunctionCtx| x.abs();
                let res_arr =
                    UnaryExecutor::eval_batch_lazy_select::<I64Array, I64Array, _>(arr, f)?;
                Ok(res_arr)
            }
            ArrayImpl::Float64(_) => {
                let f = |x: &F64, _: &mut FunctionCtx| x.abs().into();
                let res_arr =
                    UnaryExecutor::eval_batch_lazy_select::<F64Array, F64Array, _>(arr, f)?;
                Ok(res_arr)
            }
            _ => Err(FunctionError::InvalidDataTypes(
                "TODO: Support more type".to_string(),
            )),
        }
    }
}
