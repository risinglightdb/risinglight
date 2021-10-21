use crate::array::ArrayImpl;
use crate::storage::RowHandler;

pub struct SecondaryRowHandler {}

impl RowHandler for SecondaryRowHandler {
    fn from_column(_column: &ArrayImpl, _idx: usize) -> Self {
        Self {}
    }
}
