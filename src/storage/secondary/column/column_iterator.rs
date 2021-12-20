use super::{
    BoolColumnIterator, CharBlockIteratorFactory, CharColumnIterator, Column, ColumnIterator,
    DecimalColumnIterator, F64ColumnIterator, I32ColumnIterator, PrimitiveBlockIteratorFactory,
};
use crate::array::{Array, ArrayImpl};
use crate::catalog::ColumnCatalog;
use crate::types::DataTypeKind;

/// [`ColumnIteratorImpl`] of all types
pub enum ColumnIteratorImpl {
    Int32(I32ColumnIterator),
    Float64(F64ColumnIterator),
    Bool(BoolColumnIterator),
    Char(CharColumnIterator),
    Decimal(DecimalColumnIterator),
}

impl ColumnIteratorImpl {
    pub async fn new(column: Column, column_info: &ColumnCatalog, start_pos: u32) -> Self {
        match column_info.datatype().kind() {
            DataTypeKind::Int(_) => Self::Int32(
                I32ColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await,
            ),
            DataTypeKind::Boolean => Self::Bool(
                BoolColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await,
            ),
            DataTypeKind::Float(_) | DataTypeKind::Double => Self::Float64(
                F64ColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await,
            ),
            DataTypeKind::Char(width) => Self::Char(
                CharColumnIterator::new(
                    column,
                    start_pos,
                    CharBlockIteratorFactory::new(width.map(|x| x as usize)),
                )
                .await,
            ),
            DataTypeKind::Varchar(width) => Self::Char(
                CharColumnIterator::new(
                    column,
                    start_pos,
                    CharBlockIteratorFactory::new(width.map(|x| x as usize)),
                )
                .await,
            ),
            DataTypeKind::Decimal(_, _) => Self::Decimal(
                DecimalColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await,
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
            Self::Decimal(it) => Self::erase_concrete_type(it.next_batch(expected_size).await),
        }
    }

    pub fn fetch_hint(&self) -> usize {
        match self {
            Self::Int32(it) => it.fetch_hint(),
            Self::Float64(it) => it.fetch_hint(),
            Self::Bool(it) => it.fetch_hint(),
            Self::Char(it) => it.fetch_hint(),
            Self::Decimal(it) => it.fetch_hint(),
        }
    }
}
