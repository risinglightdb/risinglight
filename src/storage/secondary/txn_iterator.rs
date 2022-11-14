// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use async_recursion::async_recursion;
use enum_dispatch::enum_dispatch;

use super::{ConcatIterator, MergeIterator, RowSetIterator};
use crate::array::DataChunk;
use crate::storage::{StorageChunk, StorageResult, TxnIterator};

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

impl TxnIterator for SecondaryTableTxnIterator {
    async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<DataChunk>> {
        Ok(self
            .iter
            .next_batch(expected_size)
            .await?
            .map(|x| x.to_data_chunk()))
    }
}
