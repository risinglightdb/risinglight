use super::*;
use crate::array::ArrayImpl;
use crate::types::DataValue;

mod min_max;
mod rowcount;
mod sum;

pub use min_max::*;
pub use rowcount::*;
pub use sum::*;

pub trait AggregationState: 'static + Send + Sync {
    fn update(
        &mut self,
        array: &ArrayImpl,
        visibility: Option<&Vec<bool>>,
    ) -> Result<(), ExecutorError>;

    fn output(&self) -> DataValue;
}
