use super::table::InMemoryTableInnerRef;
use super::{InMemoryRowHandler, InMemoryTable, InMemoryTxnIterator};
use crate::array::{DataChunk, DataChunkRef};
use crate::storage::{StorageColumnRef, StorageResult, Transaction};
use async_trait::async_trait;
use itertools::Itertools;

/// A transaction running on [`InMemoryStorage`].
pub struct InMemoryTransaction {
    /// Indicates whether the transaction is committed or aborted. If
    /// the [`InMemoryTransaction`] object is dropped without finishing,
    /// the transaction will panic.
    finished: bool,

    /// Includes all to-be-committed data.
    buffer: Vec<DataChunk>,

    /// When transaction is started, reference to all data chunks will
    /// be cached in `snapshot` to provide snapshot isolation.
    snapshot: Vec<DataChunkRef>,

    /// Reference to inner table.
    table: InMemoryTableInnerRef,
}

impl InMemoryTransaction {
    pub(super) fn start(table: &InMemoryTable) -> StorageResult<Self> {
        Ok(Self {
            finished: false,
            buffer: vec![],
            table: table.inner.clone(),
            snapshot: table.inner.read().unwrap().get_all_chunks()?,
        })
    }
}

#[async_trait]
impl Transaction for InMemoryTransaction {
    type TxnIteratorType = InMemoryTxnIterator;

    type RowHandlerType = InMemoryRowHandler;

    async fn scan(
        &self,
        begin_sort_key: Option<&[u8]>,
        end_sort_key: Option<&[u8]>,
        col_idx: &[StorageColumnRef],
        reversed: bool,
    ) -> StorageResult<Self::TxnIteratorType> {
        assert!(
            begin_sort_key.is_none(),
            "sort_key is not supported in InMemoryEngine for now"
        );
        assert!(
            end_sort_key.is_none(),
            "sort_key is not supported in InMemoryEngine for now"
        );
        assert!(!reversed, "reverse iterator is not supported for now");

        let col_idx = col_idx
            .iter()
            .map(|x| match x {
                StorageColumnRef::Idx(x) => *x,
                _ => panic!("column type other than user columns are not supported for now"),
            })
            .collect_vec();

        Ok(InMemoryTxnIterator::new(self.snapshot.clone(), col_idx))
    }

    async fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        self.buffer.push(columns);
        Ok(())
    }

    async fn delete(&mut self, _id: &Self::RowHandlerType) -> StorageResult<()> {
        todo!()
    }

    async fn commit(mut self) -> StorageResult<()> {
        let mut table = self.table.write().unwrap();
        for chunk in self.buffer.drain(..) {
            table.append(chunk)?;
        }
        self.finished = true;
        Ok(())
    }

    async fn abort(mut self) -> StorageResult<()> {
        self.finished = true;
        Ok(())
    }
}

impl Drop for InMemoryTransaction {
    fn drop(&mut self) {
        if !self.finished {
            panic!("Transaction dropped without committing or aborting");
        }
    }
}
