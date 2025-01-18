// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::{Array, ArrayImpl};
use crate::catalog::ColumnCatalog;
use crate::storage::secondary::column::{DateColumnIterator, IntervalColumnIterator};
use crate::types::DataType;

/// [`ColumnIteratorImpl`] of all types
pub enum ColumnIteratorImpl {
    Int16(I16ColumnIterator),
    Int32(I32ColumnIterator),
    Int64(I64ColumnIterator),
    Float64(F64ColumnIterator),
    Bool(BoolColumnIterator),
    Char(CharColumnIterator),
    Decimal(DecimalColumnIterator),
    Date(DateColumnIterator),
    Timestamp(TimestampColumnIterator),
    TimestampTz(TimestampTzColumnIterator),
    Interval(IntervalColumnIterator),
    Blob(BlobColumnIterator),
    Vector(VectorColumnIterator),
    /// Special for row handler and not correspond to any data type
    RowHandler(RowHandlerColumnIterator),
}

impl ColumnIteratorImpl {
    pub async fn new(
        column: Column,
        column_info: &ColumnCatalog,
        start_pos: u32,
    ) -> StorageResult<Self> {
        use DataType::*;
        let iter = match column_info.data_type() {
            Null => panic!("column type should not be null"),
            Int16 => Self::Int16(
                I16ColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await?,
            ),
            Int32 => Self::Int32(
                I32ColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await?,
            ),
            Int64 => Self::Int64(
                I64ColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await?,
            ),
            Bool => Self::Bool(
                BoolColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await?,
            ),
            Float64 => Self::Float64(
                F64ColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await?,
            ),
            String => Self::Char(
                CharColumnIterator::new(column, start_pos, CharBlockIteratorFactory::new(None))
                    .await?,
            ),
            Decimal(_, _) => Self::Decimal(
                DecimalColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await?,
            ),
            Date => Self::Date(
                DateColumnIterator::new(column, start_pos, PrimitiveBlockIteratorFactory::new())
                    .await?,
            ),
            Timestamp => Self::Timestamp(
                TimestampColumnIterator::new(
                    column,
                    start_pos,
                    PrimitiveBlockIteratorFactory::new(),
                )
                .await?,
            ),
            TimestampTz => Self::TimestampTz(
                TimestampTzColumnIterator::new(
                    column,
                    start_pos,
                    PrimitiveBlockIteratorFactory::new(),
                )
                .await?,
            ),
            Interval => Self::Interval(
                IntervalColumnIterator::new(
                    column,
                    start_pos,
                    PrimitiveBlockIteratorFactory::new(),
                )
                .await?,
            ),
            Blob => Self::Blob(
                BlobColumnIterator::new(
                    column,
                    start_pos,
                    super::blob_column_factory::BlobBlockIteratorFactory(),
                )
                .await?,
            ),
            Vector(_) => Self::Vector(
                VectorColumnIterator::new(column, start_pos, VectorBlockIteratorFactory()).await?,
            ),
            Struct(_) => todo!("struct column iterator"),
        };
        Ok(iter)
    }

    pub fn new_row_handler(rowset_id: u32, row_count: u32, start_pos: u32) -> StorageResult<Self> {
        let iter = Self::RowHandler(RowHandlerColumnIterator::new(
            rowset_id as usize,
            row_count as usize,
            start_pos as usize,
        ));
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
    ) -> StorageResult<Option<(u32, ArrayImpl)>> {
        let result = match self {
            Self::Int16(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
            Self::Int32(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
            Self::Int64(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
            Self::Float64(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
            Self::Bool(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
            Self::Char(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
            Self::Decimal(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
            Self::Date(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
            Self::Timestamp(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
            Self::TimestampTz(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
            Self::Interval(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
            Self::Blob(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
            Self::Vector(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
            Self::RowHandler(it) => Self::erase_concrete_type(it.next_batch(expected_size).await?),
        };
        Ok(result)
    }

    pub fn fetch_hint(&self) -> (usize, bool) {
        match self {
            Self::Int16(it) => it.fetch_hint(),
            Self::Int32(it) => it.fetch_hint(),
            Self::Int64(it) => it.fetch_hint(),
            Self::Float64(it) => it.fetch_hint(),
            Self::Bool(it) => it.fetch_hint(),
            Self::Char(it) => it.fetch_hint(),
            Self::Decimal(it) => it.fetch_hint(),
            Self::Date(it) => it.fetch_hint(),
            Self::Timestamp(it) => it.fetch_hint(),
            Self::TimestampTz(it) => it.fetch_hint(),
            Self::Interval(it) => it.fetch_hint(),
            Self::Blob(it) => it.fetch_hint(),
            Self::Vector(it) => it.fetch_hint(),
            Self::RowHandler(it) => it.fetch_hint(),
        }
    }

    pub fn fetch_current_row_id(&self) -> u32 {
        match self {
            Self::Int16(it) => it.fetch_current_row_id(),
            Self::Int32(it) => it.fetch_current_row_id(),
            Self::Int64(it) => it.fetch_current_row_id(),
            Self::Float64(it) => it.fetch_current_row_id(),
            Self::Bool(it) => it.fetch_current_row_id(),
            Self::Char(it) => it.fetch_current_row_id(),
            Self::Decimal(it) => it.fetch_current_row_id(),
            Self::Date(it) => it.fetch_current_row_id(),
            Self::Timestamp(it) => it.fetch_current_row_id(),
            Self::TimestampTz(it) => it.fetch_current_row_id(),
            Self::Interval(it) => it.fetch_current_row_id(),
            Self::Blob(it) => it.fetch_current_row_id(),
            Self::Vector(it) => it.fetch_current_row_id(),
            Self::RowHandler(it) => it.fetch_current_row_id(),
        }
    }

    pub fn skip(&mut self, cnt: usize) {
        match self {
            Self::Int16(it) => it.skip(cnt),
            Self::Int32(it) => it.skip(cnt),
            Self::Int64(it) => it.skip(cnt),
            Self::Float64(it) => it.skip(cnt),
            Self::Bool(it) => it.skip(cnt),
            Self::Char(it) => it.skip(cnt),
            Self::Decimal(it) => it.skip(cnt),
            Self::Date(it) => it.skip(cnt),
            Self::Timestamp(it) => it.skip(cnt),
            Self::TimestampTz(it) => it.skip(cnt),
            Self::Interval(it) => it.skip(cnt),
            Self::Blob(it) => it.skip(cnt),
            Self::Vector(it) => it.skip(cnt),
            Self::RowHandler(it) => it.skip(cnt),
        }
    }
}
