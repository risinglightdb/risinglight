use crate::array::DataChunk;
use crate::storage::{StorageResult, TxnIterator};
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;

use super::ConcatIterator;

#[enum_dispatch]
pub enum SecondaryIterator {
    Concat(ConcatIterator),
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

#[async_trait]
impl TxnIterator for SecondaryTableTxnIterator {
    async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<DataChunk>> {
        Ok(match &mut self.iter {
            SecondaryIterator::Concat(iter) => iter
                .next_batch(expected_size)
                .await
                .map(|x| x.to_data_chunk()),
        })
    }
}
