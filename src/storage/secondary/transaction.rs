use std::collections::HashMap;
use std::sync::Arc;

use super::version_manager::{Snapshot, VersionManager};
use super::{
    AddDVEntry, AddRowSetEntry, ColumnBuilderOptions, ColumnSeekPosition, ConcatIterator,
    DeleteVector, DiskRowset, EpochOp, MergeIterator, RowSetIterator, SecondaryMemRowset,
    SecondaryRowHandler, SecondaryTable, SecondaryTableTxnIterator, TransactionLock,
};
use crate::array::DataChunk;
use crate::catalog::find_sort_key_id;
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

    /// Reference table.
    table: SecondaryTable,

    /// Reference version manager.
    version: Arc<VersionManager>,

    /// Snapshot content
    snapshot: Arc<Snapshot>,

    /// Epoch of the snapshot
    epoch: u64,

    /// Rowset Id
    rowset_id: u32,

    /// Count of updated rows in this txn. If there is no insertion or updates,
    /// RowSet won't be created on disk.
    row_cnt: usize,

    delete_lock: Option<TransactionLock>,
}

impl SecondaryTransaction {
    pub(super) async fn start(
        table: &SecondaryTable,
        readonly: bool,
        update: bool,
    ) -> StorageResult<Self> {
        // pin a snapshot at version manager
        let (epoch, snapshot) = table.version.pin();

        // create memtable only if txn is not read only
        let mem = if readonly {
            None
        } else {
            Some(SecondaryMemRowset::new(table.columns.clone()))
        };

        Ok(Self {
            finished: false,
            mem,
            delete_buffer: vec![],
            table: table.clone(),
            version: table.version.clone(),
            epoch,
            snapshot,
            rowset_id: table.generate_rowset_id(),
            row_cnt: 0,
            delete_lock: if update {
                Some(table.lock_for_deletion().await)
            } else {
                None
            },
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
                    ColumnBuilderOptions::from_storage_options(&*self.table.storage_options),
                )
                .await?;

            let on_disk = DiskRowset::open(
                directory,
                self.table.columns.clone(),
                self.table.block_cache.clone(),
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

        let mut changeset = vec![];

        info!(
            "RowSet {} flushed, DV {} flushed",
            rowsets
                .iter()
                .map(|x| format!("#{}", x.rowset_id()))
                .join(","),
            dvs.iter()
                .map(|x| format!("#{}(RS{})", x.dv_id(), x.rowset_id()))
                .join(",")
        );

        // Add RowSets
        changeset.extend(rowsets.into_iter().map(|x| {
            EpochOp::AddRowSet((
                AddRowSetEntry {
                    rowset_id: x.rowset_id(),
                    table_id: self.table.table_ref_id,
                },
                x,
            ))
        }));

        // Add DVs
        changeset.extend(dvs.into_iter().map(|x| {
            EpochOp::AddDV((
                AddDVEntry {
                    rowset_id: x.rowset_id(),
                    dv_id: x.dv_id(),
                    table_id: self.table.table_ref_id,
                },
                x,
            ))
        }));

        // Commit changeset
        self.version.commit_changes(changeset).await?;

        self.finished = true;
        self.version.unpin(self.epoch);

        Ok(())
    }

    async fn scan_inner(
        &self,
        begin_sort_key: Option<&[u8]>,
        end_sort_key: Option<&[u8]>,
        col_idx: &[StorageColumnRef],
        is_sorted: bool,
        reversed: bool,
    ) -> StorageResult<SecondaryTableTxnIterator> {
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

        if let Some(rowsets) = self.snapshot.get_rowsets_of(self.table.table_id()) {
            for rowset_id in rowsets {
                let rowset = self.version.get_rowset(self.table.table_id(), *rowset_id);

                // Get DV id and read DVs
                let dvs = self
                    .snapshot
                    .get_dvs_of(self.table.table_id(), *rowset_id)
                    .map(|dvs| {
                        dvs.iter()
                            .map(|dv_id| self.version.get_dv(self.table.table_id(), *dv_id))
                            .collect_vec()
                    })
                    .unwrap_or_default();

                iters.push(
                    rowset
                        .iter(col_idx.into(), dvs, ColumnSeekPosition::start())
                        .await,
                )
            }
        }

        let final_iter = if iters.len() == 1 {
            iters.pop().unwrap().into()
        } else if is_sorted {
            let sort_key = find_sort_key_id(&self.table.columns);
            if let Some(sort_key) = sort_key {
                let real_col_idx = col_idx.iter().position(|x| match x {
                    StorageColumnRef::Idx(y) => *y as usize == sort_key,
                    _ => false,
                });
                MergeIterator::new(
                    iters.into_iter().map(|iter| iter.into()).collect_vec(),
                    real_col_idx.expect("sort key not in column list"),
                )
                .into()
            } else {
                ConcatIterator::new(iters).into()
            }
        } else {
            ConcatIterator::new(iters).into()
        };

        Ok(SecondaryTableTxnIterator::new(final_iter))
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
        self.scan_inner(begin_sort_key, end_sort_key, col_idx, is_sorted, reversed)
            .await
    }

    async fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        self.row_cnt += columns.cardinality();
        self.mem.as_mut().unwrap().append(columns).await
    }

    async fn delete(&mut self, id: &Self::RowHandlerType) -> StorageResult<()> {
        assert!(
            self.delete_lock.is_some(),
            "delete lock is not held for this txn"
        );
        self.delete_buffer.push(*id);
        Ok(())
    }

    async fn commit(mut self) -> StorageResult<()> {
        self.commit_inner().await
    }

    async fn abort(mut self) -> StorageResult<()> {
        self.finished = true;
        self.version.unpin(self.epoch);
        Ok(())
    }
}

impl Drop for SecondaryTransaction {
    fn drop(&mut self) {
        if !self.finished {
            warn!("Transaction dropped without committing or aborting");
            self.version.unpin(self.epoch);
        }
    }
}
