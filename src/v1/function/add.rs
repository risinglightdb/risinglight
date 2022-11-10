// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use rust_decimal::Decimal;

use super::*;
use crate::types::{DataType, Date, Interval, F64};
use crate::v1::binder::BindError;

pub struct AddFunction {
    return_type: DataType,
}

impl AddFunction {
    // TODO: bind scalar function
    #![allow(dead_code)]
    pub fn try_create(input_types: &[DataType]) -> Result<Box<dyn Function>, BindError> {
        if input_types.len() != 2 {
            return Err(BindError::BindFunctionError(
                "The column size should be 2".to_string(),
            ));
        }

        if input_types[0] != input_types[1] {
            return Err(BindError::BindFunctionError(
                "TODO: Support more type".to_string(),
            ));
        }

        Ok(Box::new(AddFunction {
            return_type: input_types[0].clone(),
        }))
    }
}

impl Function for AddFunction {
    fn name(&self) -> &str {
        "Add"
    }

    fn return_types(&self) -> DataType {
        self.return_type.clone()
    }

    fn execute(&self, input: &[&ArrayImpl]) -> Result<ArrayImpl, FunctionError> {
        let l_arr = input[0];
        let r_arr = input[1];
        match (&l_arr, &r_arr) {
            (ArrayImpl::Int32(_), ArrayImpl::Int32(_)) => {
                let mut check: i64 = 0;
                let f = |x: &i32, y: &i32, _: &mut FunctionCtx| {
                    let res = *x as i64 + *y as i64;
                    check |= res;
                    res as i32
                };
                let res_arr =
                    BinaryExecutor::eval_batch_lazy_select::<I32Array, I32Array, I32Array, _>(
                        l_arr, r_arr, f,
                    )?;
                if check > i32::MAX as i64 {
                    return Err(FunctionError::Overflow);
                }
                Ok(res_arr)
            }
            (ArrayImpl::Int64(_), ArrayImpl::Int64(_)) => {
                let f = |x: &i64, y: &i64, _: &mut FunctionCtx| *x + *y;
                let res_arr =
                    BinaryExecutor::eval_batch_lazy_select::<I64Array, I64Array, I64Array, _>(
                        l_arr, r_arr, f,
                    )?;
                Ok(res_arr)
            }
            (ArrayImpl::Float64(_), ArrayImpl::Float64(_)) => {
                let f = |x: &F64, y: &F64, _: &mut FunctionCtx| *x + *y;
                let res_arr =
                    BinaryExecutor::eval_batch_lazy_select::<F64Array, F64Array, F64Array, _>(
                        l_arr, r_arr, f,
                    )?;
                Ok(res_arr)
            }
            (ArrayImpl::Decimal(_), ArrayImpl::Decimal(_)) => {
                let f = |x: &Decimal, y: &Decimal, _: &mut FunctionCtx| *x + *y;
                let res_arr = BinaryExecutor::eval_batch_lazy_select::<
                    DecimalArray,
                    DecimalArray,
                    DecimalArray,
                    _,
                >(l_arr, r_arr, f)?;
                Ok(res_arr)
            }
            (ArrayImpl::Date(_), ArrayImpl::Interval(_)) => {
                let f = |x: &Date, y: &Interval, _: &mut FunctionCtx| *x + *y;
                let res_arr = BinaryExecutor::eval_batch_lazy_select::<
                    DateArray,
                    IntervalArray,
                    DateArray,
                    _,
                >(l_arr, r_arr, f)?;
                Ok(res_arr)
            }
            _ => Err(FunctionError::InvalidDataTypes(
                "TODO: Support more type".to_string(),
            )),
        }
    }
}
