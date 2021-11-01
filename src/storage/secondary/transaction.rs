use super::{
    ColumnBuilderOptions, DiskRowset, SecondaryMemRowset, SecondaryRowHandler, SecondaryTable,
    SecondaryTxnIterator,
};
use crate::array::{DataChunk, DataChunkRef};
use crate::storage::{StorageColumnRef, StorageResult, Transaction};
use async_trait::async_trait;
use itertools::Itertools;

/// A transaction running on [`SecondaryStorage`].
pub struct SecondaryTransaction {
    /// Indicates whether the transaction is committed or aborted. If
    /// the [`SecondaryTransaction`] object is dropped without finishing,
    /// the transaction will panic.
    finished: bool,

    /// Includes all to-be-committed data.
    mem: Option<SecondaryMemRowset>,

    /// When transaction is started, reference to all data chunks will
    /// be cached in `snapshot` to provide snapshot isolation.
    snapshot: Vec<DataChunkRef>,

    /// Reference table.
    table: SecondaryTable,

    /// Rowset Id
    rowset_id: usize,
}

impl SecondaryTransaction {
    /// Must not hold any inner lock to [`SecondaryTable`] when starting a transaction
    pub(super) fn start(table: &SecondaryTable) -> StorageResult<Self> {
        Ok(Self {
            finished: false,
            mem: Some(SecondaryMemRowset::new(table.info.columns.clone())),
            table: table.clone(),
            snapshot: table.snapshot()?,
            rowset_id: table.generate_rowset_id(),
        })
    }
}

#[async_trait]
impl Transaction for SecondaryTransaction {
    type TxnIteratorType = SecondaryTxnIterator;

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

        let col_idx = col_idx
            .iter()
            .map(|x| match x {
                StorageColumnRef::Idx(x) => *x,
                _ => panic!("column type other than user columns are not supported for now"),
            })
            .collect_vec();

        Ok(SecondaryTxnIterator::new(self.snapshot.clone(), col_idx))
    }

    async fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        self.mem.as_mut().unwrap().append(columns).await
    }

    async fn delete(&mut self, _id: &Self::RowHandlerType) -> StorageResult<()> {
        todo!()
    }

    async fn commit(mut self) -> StorageResult<()> {
        // flush data to disk
        let directory = self
            .mem
            .take()
            .unwrap()
            .flush(
                self.table.get_rowset_path(self.rowset_id),
                ColumnBuilderOptions::from_storage_options(&*self.table.info.storage_options),
            )
            .await?;

        // add rowset to table
        self.table.add_rowset(
            DiskRowset::open(
                directory,
                self.table.info.columns.clone(),
                self.table.info.block_cache.clone(),
                self.rowset_id,
            )
            .await?,
        )?;

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
            panic!("Transaction dropped without committing or aborting");
        }
    }
}
