// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use async_recursion::async_recursion;
use enum_dispatch::enum_dispatch;
use futures_async_stream::try_stream;

use super::{ConcatIterator, MergeIterator, RowSetIterator};
use crate::array::DataChunk;
use crate::storage::{StorageChunk, StorageResult, TracedStorageError};

#[enum_dispatch]
pub enum SecondaryIterator {
    Concat(ConcatIterator),
    Merge(MergeIterator),
    RowSet(RowSetIterator),
    #[cfg(test)]
    Test(super::tests::TestIterator),
}

#[enum_dispatch(SecondaryIterator)]
pub trait SecondaryIteratorImpl {}

/// An iterator over all data in a transaction.
///
/// TODO: Lifetime of the iterator should be bound to the transaction.
/// When the transaction end, accessing items inside iterator is UB.
/// To achieve this, we must enable GAT.
pub struct SecondaryTableTxnIterator {
    iter: SecondaryIterator,
}

impl SecondaryTableTxnIterator {
    pub(super) fn new(iter: SecondaryIterator) -> Self {
        Self { iter }
    }

    #[try_stream(boxed, ok = DataChunk, error = TracedStorageError)]
    pub async fn into_stream(mut self) {
        while let Some(x) = self.iter.next_batch(None).await? {
            yield x.to_data_chunk();
        }
    }
}

impl SecondaryIterator {
    #[async_recursion]
    pub async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<StorageChunk>> {
        match self {
            SecondaryIterator::Concat(iter) => iter.next_batch(expected_size).await,
            SecondaryIterator::Merge(iter) => iter.next_batch(expected_size).await,
            SecondaryIterator::RowSet(iter) => iter.next_batch(expected_size).await,
            #[cfg(test)]
            SecondaryIterator::Test(iter) => iter.next_batch(expected_size).await,
        }
    }
}
