use crate::array::ArrayImpl;
use crate::storage::RowHandler;

pub struct InMemoryRowHandler {}

impl RowHandler for InMemoryRowHandler {
    fn from_column(_column: &ArrayImpl, _idx: usize) -> Self {
        Self {}
    }
}
