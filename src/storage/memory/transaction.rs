use std::collections::HashSet;
use std::sync::Arc;

use super::table::InMemoryTableInnerRef;
use super::{InMemoryRowHandler, InMemoryTable, InMemoryTxnIterator};
use crate::array::{DataChunk, DataChunkRef};
use crate::storage::{StorageColumnRef, StorageResult, Transaction};
use async_trait::async_trait;

/// A transaction running on `InMemoryStorage`.
pub struct InMemoryTransaction {
    /// Indicates whether the transaction is committed or aborted. If
    /// the [`InMemoryTransaction`] object is dropped without finishing,
    /// the transaction will panic.
    finished: bool,

    /// Includes all to-be-committed data.
    buffer: Vec<DataChunk>,

    /// All rows to be deleted
    delete_buffer: Vec<usize>,

    /// When transaction is started, reference to all data chunks will
    /// be cached in `snapshot` to provide snapshot isolation.
    snapshot: Arc<Vec<DataChunkRef>>,

    /// Reference to inner table.
    table: InMemoryTableInnerRef,

    /// Snapshot of all deleted rows
    deleted_rows: Arc<HashSet<usize>>,
}

impl InMemoryTransaction {
    pub(super) fn start(table: &InMemoryTable) -> StorageResult<Self> {
        let inner = table.inner.read().unwrap();
        Ok(Self {
            finished: false,
            buffer: vec![],
            delete_buffer: vec![],
            table: table.inner.clone(),
            snapshot: Arc::new(inner.get_all_chunks()),
            deleted_rows: Arc::new(inner.get_all_deleted_rows()),
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

        Ok(InMemoryTxnIterator::new(
            self.snapshot.clone(),
            self.deleted_rows.clone(),
            col_idx,
        ))
    }

    async fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        self.buffer.push(columns);
        Ok(())
    }

    async fn delete(&mut self, id: &Self::RowHandlerType) -> StorageResult<()> {
        self.delete_buffer.push(id.0 as usize);
        Ok(())
    }

    async fn commit(mut self) -> StorageResult<()> {
        let mut table = self.table.write().unwrap();
        for chunk in self.buffer.drain(..) {
            table.append(chunk)?;
        }
        for deletion in self.delete_buffer.drain(..) {
            table.delete(deletion)?;
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
