// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::Array;
use crate::types::DataTypeKind;

/// State for min or max aggregation
pub struct MinMaxAggregationState {
    result: DataValue,
    input_datatype: DataTypeKind,
    is_min: bool,
}

impl MinMaxAggregationState {
    pub fn new(input_datatype: DataTypeKind, is_min: bool) -> Self {
        Self {
            result: DataValue::Null,
            input_datatype,
            is_min,
        }
    }
}

macro_rules! min_max_func_gen {
    ($fn_name:ident, $input:ty, $result:ty, $cmp:ident) => {
        fn $fn_name(result: Option<$result>, input: Option<&$input>) -> Option<$result> {
            match (result, input) {
                (_, None) => result,
                (None, Some(i)) => Some(<$result>::from(*i)),
                (Some(r), Some(i)) => Some(std::cmp::$cmp(r, <$result>::from(*i))),
            }
        }
    };
}

min_max_func_gen!(min_i32, i32, i32, min);
min_max_func_gen!(max_i32, i32, i32, max);
min_max_func_gen!(min_i64, i64, i64, min);
min_max_func_gen!(max_i64, i64, i64, max);

impl AggregationState for MinMaxAggregationState {
    fn update(&mut self, array: &ArrayImpl) -> Result<(), ExecutorError> {
        match (array, &self.input_datatype) {
            (ArrayImpl::Int32(arr), DataTypeKind::Int32) => {
                let temp = arr
                    .iter()
                    .fold(None, if self.is_min { min_i32 } else { max_i32 });
                if let Some(val) = temp {
                    self.result = match self.result {
                        DataValue::Null => DataValue::Int32(val),
                        DataValue::Int32(res) if self.is_min => DataValue::Int32(res.min(val)),
                        DataValue::Int32(res) => DataValue::Int32(res.max(val)),
                        _ => panic!("Mismatched type"),
                    };
                }
            }
            (ArrayImpl::Int64(arr), DataTypeKind::Int64) => {
                let temp = arr
                    .iter()
                    .fold(None, if self.is_min { min_i64 } else { max_i64 });
                if let Some(val) = temp {
                    self.result = match self.result {
                        DataValue::Null => DataValue::Int64(val),
                        DataValue::Int64(res) if self.is_min => DataValue::Int64(res.min(val)),
                        DataValue::Int64(res) => DataValue::Int64(res.max(val)),
                        _ => panic!("Mismatched type"),
                    };
                }
            }
            _ => panic!("Mismatched type"),
        }
        Ok(())
    }

    fn update_single(&mut self, value: &DataValue) -> Result<(), ExecutorError> {
        match (value, &self.input_datatype) {
            (DataValue::Int32(val), DataTypeKind::Int32) => {
                self.result = match self.result {
                    DataValue::Null => DataValue::Int32(*val),
                    DataValue::Int32(res) if self.is_min => DataValue::Int32(res.min(*val)),
                    DataValue::Int32(res) => DataValue::Int32(res.max(*val)),
                    _ => panic!("Mismatched type"),
                };
            }
            (DataValue::Int64(val), DataTypeKind::Int64) => {
                self.result = match self.result {
                    DataValue::Null => DataValue::Int64(*val),
                    DataValue::Int64(res) if self.is_min => DataValue::Int64(res.min(*val)),
                    DataValue::Int64(res) => DataValue::Int64(res.max(*val)),
                    _ => panic!("Mismatched type"),
                };
            }
            _ => panic!("Mismatched type"),
        }
        Ok(())
    }

    fn output(&self) -> DataValue {
        self.result.clone()
    }
}
