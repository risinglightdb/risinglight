// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use risinglight_proto::rowset::block_statistics::BlockStatisticsType;
use risinglight_proto::rowset::DeleteRecord;
use tokio::sync::OwnedMutexGuard;
use tracing::{info, warn};

use super::version_manager::{Snapshot, Version, VersionManager};
use super::{
    AddDVEntry, AddRowSetEntry, ColumnBuilderOptions, ConcatIterator, DeleteVector, DiskRowset,
    EpochOp, MergeIterator, RowSetIterator, SecondaryMemRowsetImpl, SecondaryRowHandler,
    SecondaryTable, SecondaryTableTxnIterator,
};
use crate::array::DataChunk;
use crate::catalog::find_sort_key_id;
use crate::storage::secondary::statistics::create_statistics_global_aggregator;
use crate::storage::{StorageColumnRef, StorageResult, Transaction};
use crate::types::DataValue;
use crate::v1::binder::BoundExpr;

/// A transaction running on `SecondaryStorage`.
pub struct SecondaryTransaction {
    /// Indicates whether the transaction is committed or aborted. If
    /// the [`SecondaryTransaction`] object is dropped without finishing,
    /// the transaction will panic.
    finished: bool,

    /// Includes all to-be-committed data.
    mem: Option<SecondaryMemRowsetImpl>,

    /// Includes all to-be-deleted rows
    delete_buffer: Vec<SecondaryRowHandler>,

    /// Reference table.
    table: SecondaryTable,

    /// Reference version manager.
    version: Arc<VersionManager>,

    /// Snapshot content
    snapshot: Arc<Snapshot>,

    /// The rowsets produced in the txn.
    to_be_committed_rowsets: Vec<DiskRowset>,

    delete_lock: Option<OwnedMutexGuard<()>>,

    read_only: bool,

    /// Total size of written data in the current txn
    ///
    /// TODO: we only calculate batch insert here. Need to estimate delete vector size.
    total_size: usize,

    /// Reference version.
    _pin_version: Arc<Version>,
}

impl SecondaryTransaction {
    /// Start a transaction on Secondary. If `update` is set to true, we will hold the delete lock
    /// of a table.
    pub(super) async fn start(
        table: &SecondaryTable,
        read_only: bool,
        update: bool,
    ) -> StorageResult<Self> {
        // pin a snapshot at version manager
        let pin_version = table.version.pin();
        Ok(Self {
            finished: false,
            mem: None,
            delete_buffer: vec![],
            table: table.clone(),
            version: table.version.clone(),
            snapshot: pin_version.snapshot.clone(),
            delete_lock: if update {
                Some(table.lock_for_deletion().await)
            } else {
                None
            },
            to_be_committed_rowsets: vec![],
            read_only,
            total_size: 0,
            _pin_version: pin_version,
        })
    }

    async fn flush_rowset(&mut self) -> StorageResult<()> {
        // only flush when we have memtables
        let mem = if let Some(mem) = self.mem.take() {
            mem
        } else {
            return Ok(());
        };
        let rowset_id = mem.get_rowset_id();
        let directory = self.table.get_rowset_path(rowset_id);

        // flush data to disk
        mem.flush(self.table.storage_options.io_backend.clone(), &directory)
            .await?;

        let on_disk = DiskRowset::open(
            directory,
            self.table.columns.clone(),
            self.table.block_cache.clone(),
            rowset_id,
            self.table.storage_options.io_backend.clone(),
        )
        .await?;

        self.to_be_committed_rowsets.push(on_disk);

        Ok(())
    }

    async fn commit_inner(mut self) -> StorageResult<()> {
        self.flush_rowset().await?;

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

        let rowsets = std::mem::take(&mut self.to_be_committed_rowsets);

        let mut dvs = vec![];
        for (rowset_id, deletes) in delete_split_map {
            let dv_id = self.table.generate_dv_id();
            use bytes::Bytes;

            use super::IOBackend;
            let path = self.table.get_dv_path(rowset_id, dv_id);
            match &self.table.storage_options.io_backend {
                IOBackend::InMemory(map) => {
                    let mut buf = vec![];
                    DeleteVector::write_all(&mut buf, &deletes).await?;
                    let mut guard = map.lock();
                    guard.insert(path, Bytes::from(buf));
                }
                _ => {
                    let mut file = tokio::fs::OpenOptions::default()
                        .write(true)
                        .create_new(true)
                        .open(path)
                        .await?;
                    DeleteVector::write_all(&mut file, &deletes).await?;
                    file.sync_data().await?;
                }
            }
            dvs.push(DeleteVector::new(dv_id, rowset_id, deletes));
        }

        let mut changeset = vec![];

        match rowsets[..] {
            [] => {
                info!(
                    "DV {} flushed",
                    dvs.iter()
                        .map(|x| format!("#{}(RS{})", x.dv_id(), x.rowset_id()))
                        .join(",")
                );
            }
            _ => {
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
            }
        }

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

        Ok(())
    }

    async fn scan_inner(
        &self,
        begin_keys: &[DataValue],
        end_keys: &[DataValue],
        col_idx: &[StorageColumnRef],
        is_sorted: bool,
        reversed: bool,
        expr: Option<BoundExpr>,
    ) -> StorageResult<SecondaryTableTxnIterator> {
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

                let start_rowid = rowset.start_rowid(begin_keys).await;
                iters.push(
                    rowset
                        .iter(
                            col_idx.into(),
                            dvs,
                            start_rowid,
                            expr.clone(),
                            begin_keys,
                            end_keys,
                        )
                        .await?,
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
                    vec![real_col_idx.expect("sort key not in column list")],
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

    /// Aggregate block statistics of one column. In the future, we might support predicate
    /// push-down, and this function will add filter-scan-aggregate functionality.
    ///
    /// This function can gather multiple statistics at a time (in the future).
    pub fn aggreagate_block_stat(
        &self,
        ty: &[(BlockStatisticsType, StorageColumnRef)],
    ) -> Vec<DataValue> {
        let mut agg = ty
            .iter()
            .map(|(ty, _)| create_statistics_global_aggregator(*ty))
            .collect_vec();

        if let Some(rowsets) = self.snapshot.get_rowsets_of(self.table.table_id()) {
            for rowset_id in rowsets {
                let rowset = self.version.get_rowset(self.table.table_id(), *rowset_id);
                for ((_, col_idx), agg) in ty.iter().zip(agg.iter_mut()) {
                    let user_col_idx = match col_idx {
                        StorageColumnRef::Idx(idx) => idx,
                        _ => panic!("unsupported column ref for block aggregation"),
                    };
                    let column = rowset.column(*user_col_idx as usize);
                    agg.apply_batch(column.index());
                }
            }
        }

        agg.into_iter().map(|agg| agg.get_output()).collect_vec()
    }

    pub async fn append_inner(&mut self, columns: DataChunk) -> StorageResult<()> {
        if self.read_only {
            panic!("Txn is read-only but append is called");
        }
        if self.mem.is_none() {
            let rowset_id = self.table.generate_rowset_id();
            let directory = self.table.get_rowset_path(rowset_id);

            if !self.table.storage_options.disable_all_disk_operation {
                tokio::fs::create_dir(&directory).await?;
            }

            self.mem = Some(SecondaryMemRowsetImpl::new(
                self.table.columns.clone(),
                ColumnBuilderOptions::from_storage_options(&self.table.storage_options),
                rowset_id,
            ));
        }
        let mem = self.mem.as_mut().unwrap();
        self.total_size += columns.estimated_size();
        mem.append(columns).await?;
        if self.total_size >= self.table.storage_options.target_rowset_size {
            if self.total_size >= self.table.storage_options.target_rowset_size * 2 {
                warn!("DataChunk is too big, target_row_size exceed 2x limit.")
            }
            self.total_size = 0;
            self.flush_rowset().await?;
        }
        Ok(())
    }
}

impl Transaction for SecondaryTransaction {
    type TxnIteratorType = SecondaryTableTxnIterator;

    type RowHandlerType = SecondaryRowHandler;

    async fn scan(
        &self,
        begin_sort_key: &[DataValue],
        end_sort_key: &[DataValue],
        col_idx: &[StorageColumnRef],
        is_sorted: bool,
        reversed: bool,
        expr: Option<BoundExpr>,
    ) -> StorageResult<SecondaryTableTxnIterator> {
        self.scan_inner(
            begin_sort_key,
            end_sort_key,
            col_idx,
            is_sorted,
            reversed,
            expr,
        )
        .await
    }

    async fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        self.append_inner(columns).await
    }

    async fn delete(&mut self, id: &Self::RowHandlerType) -> StorageResult<()> {
        assert!(
            self.delete_lock.is_some(),
            "delete lock is not held for this txn"
        );
        self.delete_buffer.push(*id);
        Ok(())
    }

    async fn commit(self) -> StorageResult<()> {
        self.commit_inner().await
    }

    async fn abort(mut self) -> StorageResult<()> {
        self.finished = true;
        Ok(())
    }
}
