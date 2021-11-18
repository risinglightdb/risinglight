use super::*;
use crate::array::ArrayImpl;
use crate::types::DataValue;

mod min_max;
mod rowcount;
mod sum;
mod count;

pub use min_max::*;
pub use rowcount::*;
pub use sum::*;
pub use count::*;

/// `AggregationState` records the state of an aggregation
pub trait AggregationState: 'static + Send + Sync {
    fn update(&mut self, array: &ArrayImpl) -> Result<(), ExecutorError>;

    fn update_single(&mut self, value: &DataValue) -> Result<(), ExecutorError>;

    fn output(&self) -> DataValue;
}
