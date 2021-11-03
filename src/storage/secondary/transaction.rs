use std::sync::Arc;

use super::{
    ColumnBuilderOptions, ColumnSeekPosition, ConcatIterator, DiskRowset, RowSetIterator,
    SecondaryMemRowset, SecondaryRowHandler, SecondaryTable, SecondaryTableTxnIterator,
};
use crate::array::DataChunk;
use crate::storage::{StorageColumnRef, StorageResult, Transaction};
use async_trait::async_trait;

/// A transaction running on [`SecondaryStorage`].
pub struct SecondaryTransaction {
    /// Indicates whether the transaction is committed or aborted. If
    /// the [`SecondaryTransaction`] object is dropped without finishing,
    /// the transaction will panic.
    finished: bool,

    /// Includes all to-be-committed data.
    mem: Option<SecondaryMemRowset>,

    /// When transaction is started, the current state of the merge-tree
    /// will be recorded.
    snapshot: Vec<Arc<DiskRowset>>,

    /// Reference table.
    table: SecondaryTable,

    /// Rowset Id
    rowset_id: u32,

    /// Count of updated rows in this txn. If there is no insertion or updates,
    /// RowSet won't be created on disk.
    row_cnt: usize,
}

impl SecondaryTransaction {
    /// Must not hold any inner lock to [`SecondaryTable`] when starting a transaction
    pub(super) fn start(table: &SecondaryTable, readonly: bool) -> StorageResult<Self> {
        Ok(Self {
            finished: false,
            mem: if readonly {
                None
            } else {
                Some(SecondaryMemRowset::new(table.shared.columns.clone()))
            },
            table: table.clone(),
            snapshot: table.snapshot()?,
            rowset_id: table.generate_rowset_id(),
            row_cnt: 0,
        })
    }
}

#[async_trait]
impl Transaction for SecondaryTransaction {
    type TxnIteratorType = SecondaryTableTxnIterator;

    type RowHandlerType = SecondaryRowHandler;

    async fn scan(
        &self,
        begin_sort_key: Option<&[u8]>,
        end_sort_key: Option<&[u8]>,
        col_idx: &[StorageColumnRef],
        reversed: bool,
    ) -> StorageResult<Self::TxnIteratorType> {
        assert!(
            begin_sort_key.is_none(),
            "sort_key is not supported in SecondaryEngine for now"
        );
        assert!(
            end_sort_key.is_none(),
            "sort_key is not supported in SecondaryEngine for now"
        );
        assert!(!reversed, "reverse iterator is not supported for now");

        let mut iters: Vec<RowSetIterator> = vec![];
        for rowset in &self.snapshot {
            iters.push(
                rowset
                    .iter(col_idx.into(), ColumnSeekPosition::start())
                    .await,
            )
        }
        Ok(SecondaryTableTxnIterator::new(
            ConcatIterator::new(iters).into(),
        ))
    }

    async fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        self.row_cnt += columns.cardinality();
        self.mem.as_mut().unwrap().append(columns).await
    }

    async fn delete(&mut self, _id: &Self::RowHandlerType) -> StorageResult<()> {
        todo!()
    }

    async fn commit(mut self) -> StorageResult<()> {
        if self.row_cnt > 0 {
            let directory = self.table.get_rowset_path(self.rowset_id);

            tokio::fs::create_dir(&directory).await.ok();

            // flush data to disk
            self.mem
                .take()
                .unwrap()
                .flush(
                    &directory,
                    ColumnBuilderOptions::from_storage_options(&*self.table.shared.storage_options),
                )
                .await?;

            // add rowset to table
            self.table
                .add_rowset(
                    DiskRowset::open(
                        directory,
                        self.table.shared.columns.clone(),
                        self.table.shared.block_cache.clone(),
                        self.rowset_id,
                    )
                    .await?,
                )
                .await?;
        }

        self.finished = true;

        Ok(())
    }

    async fn abort(mut self) -> StorageResult<()> {
        self.finished = true;
        Ok(())
    }
}

impl Drop for SecondaryTransaction {
    fn drop(&mut self) {
        if !self.finished {
            warn!("Transaction dropped without committing or aborting");
        }
    }
}
