use crate::{
    array::{Array, ArrayImpl},
    catalog::ColumnCatalog,
    types::DataTypeKind,
};

use super::{
    BoolColumnIterator, CharColumnIterator, Column, ColumnIterator, F64ColumnIterator,
    I32ColumnIterator,
};

/// [`ColumnIteratorImpl`] of all types
pub enum ColumnIteratorImpl {
    Int32(I32ColumnIterator),
    Float64(F64ColumnIterator),
    Bool(BoolColumnIterator),
    Char(CharColumnIterator),
}

impl ColumnIteratorImpl {
    pub async fn new(column: Column, column_info: &ColumnCatalog, start_pos: u32) -> Self {
        match column_info.datatype().kind() {
            DataTypeKind::Int(_) => Self::Int32(I32ColumnIterator::new(column, start_pos).await),
            DataTypeKind::Boolean => Self::Bool(BoolColumnIterator::new(column, start_pos).await),
            DataTypeKind::Float(_) | DataTypeKind::Double => {
                Self::Float64(F64ColumnIterator::new(column, start_pos).await)
            }
            DataTypeKind::Char(width) => Self::Char(
                CharColumnIterator::new(column, start_pos, width.map(|x| x as usize)).await,
            ),
            DataTypeKind::Varchar(width) => Self::Char(
                CharColumnIterator::new(column, start_pos, width.map(|x| x as usize)).await,
            ),
            other_datatype => todo!(
                "column iterator for {:?} is not implemented",
                other_datatype
            ),
        }
    }

    fn erase_concrete_type(
        ret: Option<(u32, impl Array + Into<ArrayImpl>)>,
    ) -> Option<(u32, ArrayImpl)> {
        ret.map(|(row_id, array)| (row_id, array.into()))
    }

    pub async fn next_batch(&mut self, expected_size: Option<usize>) -> Option<(u32, ArrayImpl)> {
        match self {
            Self::Int32(it) => Self::erase_concrete_type(it.next_batch(expected_size).await),
            Self::Float64(it) => Self::erase_concrete_type(it.next_batch(expected_size).await),
            Self::Bool(it) => Self::erase_concrete_type(it.next_batch(expected_size).await),
            Self::Char(it) => Self::erase_concrete_type(it.next_batch(expected_size).await),
        }
    }

    pub fn fetch_hint(&self) -> usize {
        match self {
            Self::Int32(it) => it.fetch_hint(),
            Self::Float64(it) => it.fetch_hint(),
            Self::Bool(it) => it.fetch_hint(),
            Self::Char(it) => it.fetch_hint(),
        }
    }
}
