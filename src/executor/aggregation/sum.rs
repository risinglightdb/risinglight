use super::*;
#[allow(unused_imports)]
use crate::array::{Array, ArrayValidExt};
use crate::types::DataTypeKind;

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
    ($fn_name: ident, $input: ty, $result: ty) => {
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
sum_func_gen!(sum_f64, f64, f64);

impl AggregationState for SumAggregationState {
    fn update(
        &mut self,
        array: &ArrayImpl,
        visibility: Option<&[bool]>,
    ) -> Result<(), ExecutorError> {
        let array = match visibility {
            None => array.clone(),
            Some(visibility) => {
                array.filter(visibility.iter().copied().collect::<Vec<_>>().into_iter())
            }
        };
        // TODO: refactor into macros
        match (array, &self.input_datatype) {
            (ArrayImpl::Int32(arr), DataTypeKind::Int) => {
                #[cfg(feature = "simd")]
                {
                    let bitmap = arr.get_valid_bitmap();
                    if bitmap.any() {
                        self.result = DataValue::Int32(arr.batch_iter::<32>().sum());
                    } else {
                        self.result = DataValue::Null;
                    }
                }
                #[cfg(not(feature = "simd"))]
                {
                    let mut temp: Option<i32> = None;
                    temp = arr.iter().fold(temp, sum_i32);
                    if let Some(val) = temp {
                        self.result = match self.result {
                            DataValue::Null => DataValue::Int32(val),
                            DataValue::Int32(res) => DataValue::Int32(res + val),
                            _ => panic!("Mismatched type"),
                        }
                    }
                }
            }
            (ArrayImpl::Float64(arr), DataTypeKind::Double) => {
                #[cfg(feature = "simd")]
                {
                    let bitmap = arr.get_valid_bitmap();
                    if bitmap.any() {
                        self.result = DataValue::Float64(arr.batch_iter::<32>().sum());
                    } else {
                        self.result = DataValue::Null;
                    }
                }
                #[cfg(not(feature = "simd"))]
                {
                    let mut temp: Option<f64> = None;
                    temp = arr.iter().fold(temp, sum_f64);
                    if let Some(val) = temp {
                        self.result = match self.result {
                            DataValue::Null => DataValue::Float64(val),
                            DataValue::Float64(res) => DataValue::Float64(res + val),
                            _ => panic!("Mismatched type"),
                        }
                    }
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
    use crate::array::ArrayBuilderImpl;
    use crate::types::DataType;

    #[test]
    fn test_sum() {
        let mut state = SumAggregationState::new(DataTypeKind::Int);
        let array = ArrayImpl::Int32((1..5).collect());
        state.update(&array, None).unwrap();
        assert_eq!(state.output(), DataValue::Int32(10));

        let mut state = SumAggregationState::new(DataTypeKind::Double);
        let mut builder = ArrayBuilderImpl::new(&DataType::new(DataTypeKind::Double, false));
        for i in [0.1, 0.2, 0.3, 0.4].iter() {
            builder.push(&DataValue::Float64(*i));
        }
        let array = builder.finish();
        state.update(&array, None).unwrap();
        assert_eq!(state.output(), DataValue::Float64(1.));
    }
}
