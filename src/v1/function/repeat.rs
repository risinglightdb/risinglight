// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::types::{DataType, DataTypeKind};
use crate::v1::binder::BindError;

#[inline]
pub fn repeat(v: impl AsRef<[u8]>, times: usize) -> Vec<u8> {
    v.as_ref().repeat(times)
}

pub struct RepeatFunction {
    return_type: DataType,
}

impl RepeatFunction {
    // TODO: bind scalar function
    #![allow(dead_code)]
    pub fn try_create(input_types: &[DataType]) -> Result<Box<dyn Function>, BindError> {
        if input_types.len() != 2 {
            return Err(BindError::BindFunctionError(
                "The column size should be 2".to_string(),
            ));
        }

        if input_types[0].kind != DataTypeKind::String {
            return Err(BindError::BindFunctionError(
                "Only support VARCHAR".to_string(),
            ));
        }

        if input_types[1].kind != DataTypeKind::Int64 || input_types[1].kind != DataTypeKind::Int32
        {
            return Err(BindError::BindFunctionError(
                "TODO: Only support [i32, i64], Need support more type".to_string(),
            ));
        }

        Ok(Box::new(RepeatFunction {
            return_type: input_types[0].clone(),
        }))
    }
}

impl Function for RepeatFunction {
    fn name(&self) -> &str {
        "Repeat"
    }

    fn return_types(&self) -> DataType {
        self.return_type.clone()
    }

    fn execute(&self, input: &[&ArrayImpl]) -> Result<ArrayImpl, FunctionError> {
        let l_arr = input[0];
        let r_arr = input[1];

        match (&l_arr, &r_arr) {
            (ArrayImpl::Utf8(_), ArrayImpl::Int32(_)) => {
                let f = |x: &str, t: &i32, _: &mut FunctionCtx| {
                    let u8s = repeat(x, *t as usize);
                    unsafe { String::from_utf8_unchecked(u8s) }
                };
                let res_arr =
                    BinaryExecutor::eval_batch_standard::<Utf8Array, I32Array, Utf8Array, _>(
                        l_arr, r_arr, f,
                    )?;
                Ok(res_arr)
            }
            (ArrayImpl::Utf8(_), ArrayImpl::Int64(_)) => {
                let f = |x: &str, t: &i64, _: &mut FunctionCtx| {
                    let u8s = repeat(x, *t as usize);
                    unsafe { String::from_utf8_unchecked(u8s) }
                };
                let res_arr =
                    BinaryExecutor::eval_batch_standard::<Utf8Array, I64Array, Utf8Array, _>(
                        l_arr, r_arr, f,
                    )?;
                Ok(res_arr)
            }
            _ => Err(FunctionError::InvalidDataTypes(
                "TODO: Support more type".to_string(),
            )),
        }
    }
}
