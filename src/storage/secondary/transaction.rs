use std::collections::HashMap;
use std::sync::Arc;

use super::rowset::find_sort_key_id;
use super::{
    ColumnBuilderOptions, ColumnSeekPosition, ConcatIterator, DeleteVector, DiskRowset,
    MergeIterator, RowSetIterator, SecondaryMemRowset, SecondaryRowHandler, SecondaryTable,
    SecondaryTableTxnIterator,
};
use crate::array::DataChunk;
use crate::storage::{StorageColumnRef, StorageResult, Transaction};
use async_trait::async_trait;
use itertools::Itertools;
use risinglight_proto::rowset::DeleteRecord;

/// A transaction running on `SecondaryStorage`.
pub struct SecondaryTransaction {
    /// Indicates whether the transaction is committed or aborted. If
    /// the [`SecondaryTransaction`] object is dropped without finishing,
    /// the transaction will panic.
    finished: bool,

    /// Includes all to-be-committed data.
    mem: Option<SecondaryMemRowset>,

    /// Includes all to-be-deleted rows
    delete_buffer: Vec<SecondaryRowHandler>,

    /// When transaction is started, the current state of the merge-tree
    /// will be recorded.
    snapshot: Vec<Arc<DiskRowset>>,

    /// Snapshot of all committed deletes
    dv_snapshot: HashMap<u32, Vec<Arc<DeleteVector>>>,

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
        let inner = table.inner.read();
        Ok(Self {
            finished: false,
            mem: if readonly {
                None
            } else {
                Some(SecondaryMemRowset::new(table.shared.columns.clone()))
            },
            delete_buffer: vec![],
            table: table.clone(),
            snapshot: inner.on_disk.values().cloned().collect_vec(),
            rowset_id: table.generate_rowset_id(),
            dv_snapshot: inner.dv.clone(),
            row_cnt: 0,
        })
    }

    async fn commit_inner(mut self) -> StorageResult<()> {
        let rowsets = if self.row_cnt > 0 {
            let directory = self.table.get_rowset_path(self.rowset_id);

            tokio::fs::create_dir(&directory).await.unwrap();

            // flush data to disk
            self.mem
                .take()
                .unwrap()
                .flush(
                    &directory,
                    ColumnBuilderOptions::from_storage_options(&*self.table.shared.storage_options),
                )
                .await?;

            let on_disk = DiskRowset::open(
                directory,
                self.table.shared.columns.clone(),
                self.table.shared.block_cache.clone(),
                self.rowset_id,
            )
            .await?;

            vec![on_disk]
        } else {
            vec![]
        };

        // flush deletes to disk
        let mut delete_split_map = HashMap::new();
        for delete in self.delete_buffer.drain(..) {
            delete_split_map
                .entry(delete.rowset_id())
                .or_insert_with(Vec::new)
                .push(DeleteRecord {
                    row_id: delete.row_id(),
                });
        }

        let mut dvs = vec![];
        for (rowset_id, deletes) in delete_split_map {
            let dv_id = self.table.generate_dv_id();
            let mut file = tokio::fs::OpenOptions::default()
                .write(true)
                .create_new(true)
                .open(self.table.get_dv_path(rowset_id, dv_id))
                .await?;
            DeleteVector::write_all(&mut file, &deletes).await?;
            file.sync_data().await?;
            dvs.push(DeleteVector::new(dv_id, rowset_id, deletes));
        }

        // commit all changes
        self.table.commit(rowsets, dvs, vec![]).await?;

        self.finished = true;

        Ok(())
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
        is_sorted: bool,
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
                    .iter(
                        col_idx.into(),
                        self.dv_snapshot
                            .get(&rowset.rowset_id())
                            .cloned()
                            .unwrap_or_default(),
                        ColumnSeekPosition::start(),
                    )
                    .await,
            )
        }

        let final_iter = if iters.len() == 1 {
            iters.pop().unwrap().into()
        } else if is_sorted {
            let sort_key = find_sort_key_id(&self.table.shared.columns);
            if let Some(sort_key) = sort_key {
                let real_col_idx = col_idx.iter().position(|x| match x {
                    StorageColumnRef::Idx(y) => *y as usize == sort_key,
                    _ => false,
                });
                MergeIterator::new(iters, real_col_idx.expect("sort key not in column list")).into()
            } else {
                ConcatIterator::new(iters).into()
            }
        } else {
            ConcatIterator::new(iters).into()
        };

        Ok(SecondaryTableTxnIterator::new(final_iter))
    }

    async fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        self.row_cnt += columns.cardinality();
        self.mem.as_mut().unwrap().append(columns).await
    }

    async fn delete(&mut self, id: &Self::RowHandlerType) -> StorageResult<()> {
        self.delete_buffer.push(*id);
        Ok(())
    }

    async fn commit(mut self) -> StorageResult<()> {
        self.commit_inner().await
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
