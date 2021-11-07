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
// TODO: To support min and max on `f64`, we should implement std::cmp::Ord for `f64`

impl AggregationState for MinMaxAggregationState {
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
        match (array, &self.input_datatype) {
            (ArrayImpl::Int32(arr), DataTypeKind::Int) => {
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
            _ => panic!("Mismatched type"),
        }
        Ok(())
    }

    fn output(&self) -> DataValue {
        self.result.clone()
    }
}
