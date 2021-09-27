use super::*;
use crate::array::{Array, ArrayImpl};
use crate::types::{DataTypeKind, DataValue};

pub trait AggregationState {
    fn update(&mut self, array: &ArrayImpl) -> Result<(), ExecutorError>;

    fn output(&self) -> DataValue;
}

pub struct SumAggregationState {
    result: DataValue,
    input_datatype: DataTypeKind,
}

#[allow(dead_code)]
impl SumAggregationState {
    pub fn new(input_datatype: DataTypeKind) -> Box<Self> {
        Box::new(Self {
            result: DataValue::Null,
            input_datatype,
        })
    }
}

macro_rules! sum_func_gen {
    ($fn_name: ident, $input: ty, $result: ty) => {
        fn $fn_name(result: Option<$result>, input: Option<&$input>) -> Option<$result> {
            match (result, input) {
                (_, None) => result,
                (None, Some(i)) => Some(<$result>::from(*i)),
                (Some(r), Some(i)) => Some(r + <$result>::from(*i)),
            }
        }
    };
}

sum_func_gen!(sum_i32, i32, i64);

impl AggregationState for SumAggregationState {
    fn update(&mut self, array: &ArrayImpl) -> Result<(), ExecutorError> {
        match (array, &self.input_datatype) {
            (ArrayImpl::Int32(arr), DataTypeKind::Int) => {
                let mut temp: Option<i64> = None;
                temp = arr.iter().fold(temp, sum_i32);
                match temp {
                    None => self.result = DataValue::Null,
                    Some(val) => self.result = DataValue::Int64(val),
                }
            }
            _ => todo!("Support more types for aggregation."),
        }
        Ok(())
    }

    fn output(&self) -> DataValue {
        self.result.clone()
    }
}

/// TODO: remove the tests after supporting end-2-end queries.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ArrayBuilderImpl;
    use crate::types::{DataType, DataTypeKind};
    #[test]
    fn test_sum() {
        let mut state = SumAggregationState::new(DataTypeKind::Int);
        let mut builder = ArrayBuilderImpl::new(DataType::new(DataTypeKind::Int, true));
        builder.push(&DataValue::Int32(1));
        builder.push(&DataValue::Int32(2));
        builder.push(&DataValue::Int32(3));
        builder.push(&DataValue::Int32(4));
        let arr = builder.finish();
        state.update(&arr).unwrap();
        let val = state.output();
        match val {
            DataValue::Int64(sum) => {
                assert_eq!(sum, 10);
            }
            _ => panic!(),
        }
    }
}
