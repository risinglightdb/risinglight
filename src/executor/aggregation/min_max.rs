use super::*;
use crate::array::Array;
use crate::types::DataTypeKind;

pub struct MinMaxAggregationState {
    result: DataValue,
    input_datatype: DataTypeKind,
    is_min: bool,
}

impl MinMaxAggregationState {
    pub fn new(input_datatype: DataTypeKind, is_min: bool) -> Box<Self> {
        Box::new(Self {
            result: DataValue::Null,
            input_datatype,
            is_min,
        })
    }
}

macro_rules! min_max_func_gen {
    ($fn_name: ident, $input: ty, $result: ty, $cmp: ident) => {
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
// TODO: min requires std::cmp::Ord in `f64`
// min_max_func_gen!(min_f64, f64, f64, min);
// min_max_func_gen!(max_f64, f64, f64, max);

impl AggregationState for MinMaxAggregationState {
    fn update(
        &mut self,
        array: &ArrayImpl,
        visibility: Option<&Vec<bool>>,
    ) -> Result<(), ExecutorError> {
        let array = match visibility {
            None => array.clone(),
            Some(visibility) => {
                array.filter(visibility.iter().copied().collect::<Vec<_>>().into_iter())
            }
        };
        match (array, &self.input_datatype, self.is_min) {
            (ArrayImpl::Int32(arr), DataTypeKind::Int, true) => {
                let mut temp: Option<i32> = None;
                temp = arr.iter().fold(temp, min_i32);
                match (temp, &self.result) {
                    (Some(val), DataValue::Null) => self.result = DataValue::Int32(val),
                    (Some(val), DataValue::Int32(res)) => {
                        self.result = DataValue::Int32(std::cmp::min(*res, val))
                    }
                    (None, _) => {}
                    _ => panic!("Mismatched type"),
                }
            }
            // (ArrayImpl::Float64(arr), DataTypeKind::Double, true) => {},
            (ArrayImpl::Int32(arr), DataTypeKind::Int, false) => {
                let mut temp: Option<i32> = None;
                temp = arr.iter().fold(temp, max_i32);
                match (temp, &self.result) {
                    (Some(val), DataValue::Null) => self.result = DataValue::Int32(val),
                    (Some(val), DataValue::Int32(res)) => {
                        self.result = DataValue::Int32(std::cmp::max(*res, val))
                    }
                    (None, _) => {}
                    _ => panic!("Mismatched type"),
                }
            }
            // (ArrayImpl::Float64(arr), DataTypeKind::Double, false) => {},
            _ => panic!("Mismatched type"),
        }
        Ok(())
    }

    fn output(&self) -> DataValue {
        self.result.clone()
    }
}
