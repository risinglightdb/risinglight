use super::*;
use crate::array::ArrayImpl;
use crate::function::FunctionError::{InvalidDataTypes, InvalidParameters};

pub struct AbsFunction {}

impl Function for AbsFunction {
    fn name(&self) -> &str {
        "abs"
    }

    fn return_types(
        &self,
        input_types: &[PhysicalDataTypeKind],
    ) -> Result<PhysicalDataTypeKind, FunctionError> {
        if input_types.len() != 1 {
            return Err(InvalidDataTypes("The column size should be 1".to_string()));
        }
        match &input_types[0] {
            PhysicalDataTypeKind::Int32 => Ok(PhysicalDataTypeKind::Int32),
            PhysicalDataTypeKind::Int64 => Ok(PhysicalDataTypeKind::Int64),
            PhysicalDataTypeKind::Float64 => Ok(PhysicalDataTypeKind::Float64),
            _ => Err(InvalidDataTypes("Data type is not supported".to_string())),
        }
    }

    fn execute(&self, input: &DataChunk) -> Result<DataChunk, FunctionError> {
        if input.column_count() != 1 {
            return Err(InvalidParameters("The column size should be 1".to_string()));
        }

        let arr = input.array_at(0);
        match &arr {
            ArrayImpl::Int32(i32_arr) => {
                let mut builder = I32ArrayBuilder::new();
                for val in i32_arr.iter() {
                    match val {
                        Some(val) => builder.push(Some(&(*val).abs())),
                        None => builder.push(None),
                    }
                }
                let res_arr: ArrayImpl = ArrayImpl::from(builder.finish());
                let vec = vec![res_arr];
                Ok(vec.into_iter().collect())
            }
            ArrayImpl::Int64(i64_arr) => {
                let mut builder = I64ArrayBuilder::new();
                for val in i64_arr.iter() {
                    match val {
                        Some(val) => builder.push(Some(&(*val).abs())),
                        None => builder.push(None),
                    }
                }
                let res_arr: ArrayImpl = ArrayImpl::from(builder.finish());
                let vec = vec![res_arr];
                Ok(vec.into_iter().collect())
            }
            ArrayImpl::Float64(f64_arr) => {
                let mut builder = F64ArrayBuilder::new();
                for val in f64_arr.iter() {
                    match val {
                        Some(val) => builder.push(Some(&val.abs().into())),
                        None => builder.push(None),
                    }
                }
                let res_arr: ArrayImpl = ArrayImpl::from(builder.finish());
                let vec = vec![res_arr];
                Ok(vec.into_iter().collect())
            }
            _ => Err(InvalidDataTypes("Data type is not supported".to_string())),
        }
    }
}
