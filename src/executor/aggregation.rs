use super::*;
use crate::array::{PrimitiveArrayBuilder, ArrayBuilder, ArrayImpl, DataChunk, Array};
use crate::types::{DataValue, DataTypeKind, NativeType};
use std::marker::PhantomData;
use std::convert::TryFrom;

pub trait AggregationState {
    fn update(&mut self, array: &ArrayImpl) -> Result<(), ExecutorError>;

    fn output(&self) -> DataValue;
}

pub type BoxAggState = Box<dyn AggregationState>;

struct NativeAggregationState<T, F, R>
where
    T: NativeType,
    F: FnMut(Option<R>, Option<T>) -> Option<R>,
    R: NativeType,
{
    result: Option<R>,
    input_datatype: DataTypeKind,
    result_datatype: DataTypeKind,
    f: F,
    _phantom: PhantomData<T>,
}

impl<T, F, R> NativeAggregationState<T, F, R>
where
    T: NativeType,
    F: FnMut(Option<R>, Option<T>) -> Option<R>,
    R: NativeType,
{
    fn new(f: F, input_datatype: DataTypeKind, result_datatype: DataTypeKind) -> Box<Self> {
        Box::new(Self {
            result: None,
            input_datatype,
            result_datatype,
            f,
            _phantom: PhantomData,
        })
    }
}

impl<T, F, R> AggregationState for NativeAggregationState<T, F, R>
where
    T: NativeType,
    F: FnMut(Option<R>, Option<T>) -> Option<R>,
    R: NativeType {
    fn update(&mut self, array: &ArrayImpl) -> Result<(), ExecutorError> {
        match (array, self.input_datatype) {
            (ArrayImpl::Int32(arr), DataTypeKind::Int) => { arr.iter().fold(self.result, &mut self.f); }
            _ => todo!("Support more types for aggregation.")
        }
        Ok(())
    }

    fn output(&self) -> DataValue {
       DataValue::Null
    }
}

macro_rules! sum_func_gen {
    ($fn_name: ident, $input: ty, $result: ty) => {
        fn $fn_name(result: Option<$result>, input: Option<$input>) -> Option<$result> {
            match (result, input) {
                (_, None) => result,
                (None, Some(i)) => Some(<$result>::from(i)),
                (Some(r), Some(i)) => Some(r + <$result>::from(i)),
            }
        }
    };
}

sum_func_gen!(sum_i32, i32, i64);
sum_func_gen!(sum_f64, f64, f64);
