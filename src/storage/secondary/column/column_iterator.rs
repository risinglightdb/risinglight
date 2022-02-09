// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use bitvec::prelude::BitVec;

use super::{
    BoolColumnIterator, CharBlockIteratorFactory, CharColumnIterator, Column, ColumnIterator,
    DecimalColumnIterator, F64ColumnIterator, I32ColumnIterator, PrimitiveBlockIteratorFactory,
    StorageResult,
};
use crate::array::{Array, ArrayImpl};
use crate::catalog::ColumnCatalog;
use crate::storage::secondary::column::{DateColumnIterator, IntervalColumnIterator};
use crate::types::DataTypeKind;

/// [`ColumnIteratorImpl`] of all types
pub enum ColumnIteratorImpl {
    Int32(I32ColumnIterator),
    Float64(F64ColumnIterator),
    Bool(BoolColumnIterator),
    Char(CharColumnIterator),
    Decimal(DecimalColumnIterator),
    Date(DateColumnIterator),
    Interval(IntervalColumnIterator),
}

impl ColumnIteratorImpl {
    pub async fn new(
        column: Column,
        column_info: &ColumnCatalog,
        start_pos: u32,
    ) -> StorageResult<Self> {
        let iter = match column_info.datatype().kind() {
            DataTypeKind::Int(_) => Self::Int32(
                I32ColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await?,
            ),
            DataTypeKind::Boolean => Self::Bool(
                BoolColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await?,
            ),
            DataTypeKind::Float(_) | DataTypeKind::Double => Self::Float64(
                F64ColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await?,
            ),
            DataTypeKind::Char(width) => Self::Char(
                CharColumnIterator::new(
                    column,
                    start_pos,
                    CharBlockIteratorFactory::new(width.map(|x| x as usize)),
                )
                .await?,
            ),
            DataTypeKind::Varchar(width) => Self::Char(
                CharColumnIterator::new(
                    column,
                    start_pos,
                    CharBlockIteratorFactory::new(width.map(|x| x as usize)),
                )
                .await?,
            ),
            DataTypeKind::Decimal(_, _) => Self::Decimal(
                DecimalColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await?,
            ),
            DataTypeKind::Date => Self::Date(
                DateColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await?,
            ),
            DataTypeKind::Interval => Self::Interval(
                IntervalColumnIterator::new(
                    column,
                    start_pos,
                    PrimitiveBlockIteratorFactory::new(),
                )
                .await?,
            ),
            other_datatype => todo!(
                "column iterator for {:?} is not implemented",
                other_datatype
            ),
        };
        Ok(iter)
    }

    fn erase_concrete_type(
        ret: Option<(u32, impl Array + Into<ArrayImpl>)>,
    ) -> Option<(u32, ArrayImpl)> {
        ret.map(|(row_id, array)| (row_id, array.into()))
    }

    pub async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        filter_bitmap: Option<&BitVec>,
    ) -> StorageResult<Option<(u32, ArrayImpl)>> {
        let result = match self {
            Self::Int32(it) => {
                Self::erase_concrete_type(it.next_batch(expected_size, filter_bitmap).await?)
            }
            Self::Float64(it) => {
                Self::erase_concrete_type(it.next_batch(expected_size, filter_bitmap).await?)
            }
            Self::Bool(it) => {
                Self::erase_concrete_type(it.next_batch(expected_size, filter_bitmap).await?)
            }
            Self::Char(it) => {
                Self::erase_concrete_type(it.next_batch(expected_size, filter_bitmap).await?)
            }
            Self::Decimal(it) => {
                Self::erase_concrete_type(it.next_batch(expected_size, filter_bitmap).await?)
            }
            Self::Date(it) => {
                Self::erase_concrete_type(it.next_batch(expected_size, filter_bitmap).await?)
            }
            Self::Interval(it) => {
                Self::erase_concrete_type(it.next_batch(expected_size, filter_bitmap).await?)
            }
        };
        Ok(result)
    }

    pub fn fetch_hint(&self) -> usize {
        match self {
            Self::Int32(it) => it.fetch_hint(),
            Self::Float64(it) => it.fetch_hint(),
            Self::Bool(it) => it.fetch_hint(),
            Self::Char(it) => it.fetch_hint(),
            Self::Decimal(it) => it.fetch_hint(),
            Self::Date(it) => it.fetch_hint(),
            Self::Interval(it) => it.fetch_hint(),
        }
    }

    pub fn fetch_current_row_id(&self) -> u32 {
        match self {
            Self::Int32(it) => it.fetch_current_row_id(),
            Self::Float64(it) => it.fetch_current_row_id(),
            Self::Bool(it) => it.fetch_current_row_id(),
            Self::Char(it) => it.fetch_current_row_id(),
            Self::Decimal(it) => it.fetch_current_row_id(),
            Self::Date(it) => it.fetch_current_row_id(),
            Self::Interval(it) => it.fetch_current_row_id(),
        }
    }

    pub fn skip(&mut self, cnt: usize) {
        match self {
            Self::Int32(it) => it.skip(cnt),
            Self::Float64(it) => it.skip(cnt),
            Self::Bool(it) => it.skip(cnt),
            Self::Char(it) => it.skip(cnt),
            Self::Decimal(it) => it.skip(cnt),
            Self::Date(it) => it.skip(cnt),
            Self::Interval(it) => it.skip(cnt),
        }
    }
}
