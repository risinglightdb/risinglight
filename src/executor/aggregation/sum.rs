// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use rust_decimal::Decimal;

use super::*;
use crate::array::Array;
use crate::types::{DataTypeKind, F64};

/// State for sum aggregation
pub struct SumAggregationState {
    result: DataValue,
    input_datatype: DataTypeKind,
}

impl SumAggregationState {
    pub fn new(input_datatype: DataTypeKind) -> Self {
        Self {
            result: DataValue::Null,
            input_datatype,
        }
    }
}

macro_rules! sum_func_gen {
    ($fn_name:ident, $input:ty, $result:ty) => {
        pub fn $fn_name(result: Option<$result>, input: Option<&$input>) -> Option<$result> {
            match (result, input) {
                (_, None) => result,
                (None, Some(i)) => Some(<$result>::from(*i)),
                (Some(r), Some(i)) => Some(r + <$result>::from(*i)),
            }
        }
    };
}

sum_func_gen!(sum_i32, i32, i32);
sum_func_gen!(sum_i64, i64, i64);
sum_func_gen!(sum_f64, F64, F64);
sum_func_gen!(sum_decimal, Decimal, Decimal);

impl AggregationState for SumAggregationState {
    fn update(&mut self, array: &ArrayImpl) -> Result<(), ExecutorError> {
        // TODO: refactor into macros
        match (array, &self.input_datatype) {
            (ArrayImpl::Int32(arr), DataTypeKind::Int32) => {
                let mut temp: Option<i32> = None;
                #[cfg(feature = "simd")]
                {
                    use crate::array::ArrayValidExt;
                    let bitmap = arr.get_valid_bitmap();
                    if bitmap.any() {
                        temp = Some(arr.batch_iter::<32>().sum());
                    }
                }
                #[cfg(not(feature = "simd"))]
                {
                    temp = arr.iter().fold(temp, sum_i32);
                }
                if let Some(val) = temp {
                    self.result = match self.result {
                        DataValue::Null => DataValue::Int32(val),
                        DataValue::Int32(res) => DataValue::Int32(res + val),
                        _ => panic!("Mismatched type"),
                    }
                }
            }
            (ArrayImpl::Int64(arr), DataTypeKind::Int64) => {
                let mut temp: Option<i64> = None;
                #[cfg(feature = "simd")]
                {
                    use crate::array::ArrayValidExt;
                    let bitmap = arr.get_valid_bitmap();
                    if bitmap.any() {
                        temp = Some(arr.batch_iter::<64>().sum());
                    }
                }
                #[cfg(not(feature = "simd"))]
                {
                    temp = arr.iter().fold(temp, sum_i64);
                }
                if let Some(val) = temp {
                    self.result = match self.result {
                        DataValue::Null => DataValue::Int64(val),
                        DataValue::Int64(res) => DataValue::Int64(res + val),
                        _ => panic!("Mismatched type"),
                    }
                }
            }
            (ArrayImpl::Float64(arr), DataTypeKind::Float64) => {
                let mut temp: Option<F64> = None;
                #[cfg(feature = "simd")]
                {
                    use crate::array::ArrayValidExt;
                    let bitmap = arr.get_valid_bitmap();
                    if bitmap.any() {
                        temp = Some(arr.as_native().batch_iter::<32>().sum::<f64>().into());
                    }
                }
                #[cfg(not(feature = "simd"))]
                {
                    temp = arr.iter().fold(temp, sum_f64);
                }
                if let Some(val) = temp {
                    self.result = match self.result {
                        DataValue::Null => DataValue::Float64(val),
                        DataValue::Float64(res) => DataValue::Float64(res + val),
                        _ => panic!("Mismatched type"),
                    }
                }
            }
            (ArrayImpl::Decimal(arr), DataTypeKind::Decimal(_, _)) => {
                let mut temp: Option<Decimal> = None;
                temp = arr.iter().fold(temp, sum_decimal);
                if let Some(val) = temp {
                    self.result = match self.result {
                        DataValue::Null => DataValue::Decimal(val),
                        DataValue::Decimal(res) => DataValue::Decimal(res + val),
                        _ => panic!("Mismatched type"),
                    }
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
                    DataValue::Int32(res) => DataValue::Int32(res + val),
                    _ => panic!("Mismatched type"),
                }
            }
            (DataValue::Int64(val), DataTypeKind::Int64) => {
                self.result = match self.result {
                    DataValue::Null => DataValue::Int64(*val),
                    DataValue::Int64(res) => DataValue::Int64(res + val),
                    _ => panic!("Mismatched type"),
                }
            }
            (DataValue::Float64(val), DataTypeKind::Float64) => {
                self.result = match self.result {
                    DataValue::Null => DataValue::Float64(*val),
                    DataValue::Float64(res) => DataValue::Float64(res + val),
                    _ => panic!("Mismatched type"),
                }
            }
            (DataValue::Decimal(val), DataTypeKind::Decimal(_, _)) => {
                self.result = match self.result {
                    DataValue::Null => DataValue::Decimal(*val),
                    DataValue::Decimal(res) => DataValue::Decimal(res + val),
                    _ => panic!("Mismatched type"),
                }
            }
            _ => panic!("Mismatched type"),
        }
        Ok(())
    }

    fn output(&self) -> DataValue {
        self.result.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum() {
        let mut state = SumAggregationState::new(DataTypeKind::Int32);
        let array = ArrayImpl::new_int32((1..5).collect());
        state.update(&array).unwrap();
        assert_eq!(state.output(), DataValue::Int32(10));

        let mut state = SumAggregationState::new(DataTypeKind::Int64);
        let array = ArrayImpl::new_int64((1..5).collect());
        state.update(&array).unwrap();
        assert_eq!(state.output(), DataValue::Int64(10));

        let mut state = SumAggregationState::new(DataTypeKind::Float64);
        let array = ArrayImpl::new_float64([0.1, 0.2, 0.3, 0.4].into_iter().collect());
        state.update(&array).unwrap();
        assert_eq!(state.output(), DataValue::Float64(1.0.into()));
    }
}
