use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use tokio::sync::oneshot::Receiver;

use super::{SecondaryStorage, SecondaryTable, Snapshot};
use crate::catalog::find_sort_key_id;
use crate::storage::secondary::column::ColumnSeekPosition;
use crate::storage::secondary::concat_iterator::ConcatIterator;
use crate::storage::secondary::manifest::{AddRowSetEntry, DeleteRowsetEntry};
use crate::storage::secondary::merge_iterator::MergeIterator;
use crate::storage::secondary::rowset::{DiskRowset, RowsetBuilder};
use crate::storage::secondary::version_manager::EpochOp;
use crate::storage::secondary::{ColumnBuilderOptions, SecondaryIterator};
use crate::storage::{StorageColumnRef, StorageResult};

/// Manages all compactions happening in the storage engine.
pub struct Compactor {
    storage: Arc<SecondaryStorage>,
    stop: Receiver<()>,
}

impl Compactor {
    pub fn new(storage: Arc<SecondaryStorage>, stop: Receiver<()>) -> Self {
        Self { storage, stop }
    }

    async fn compact_table(&self, snapshot: &Snapshot, table: SecondaryTable) -> StorageResult<()> {
        let rowsets = if let Some(rowsets) = snapshot.get_rowsets_of(table.table_id()) {
            rowsets
        } else {
            // No rowset available for this table
            return Ok(());
        };
        let mut selected_rowsets = vec![];
        let mut current_size = 0;
        for rowset_id in rowsets {
            let rowset = self
                .storage
                .version
                .get_rowset(table.table_id(), *rowset_id);
            let on_disk_size = rowset.on_disk_size();
            if on_disk_size + current_size <= self.storage.options.target_rowset_size as u64 {
                current_size += on_disk_size;
                selected_rowsets.push(rowset);
            }
        }
        if selected_rowsets.len() <= 1 {
            return Ok(());
        }

        // sort RowSets by id so that the output RowSet will have old rows in the front and new rows
        // at the end.
        selected_rowsets.sort_by_key(|x| x.rowset_id());

        let column_refs: Arc<[StorageColumnRef]> = (0..table.columns.len())
            .map(|idx| StorageColumnRef::Idx(idx as u32))
            .collect_vec()
            .into();
        let mut iters = vec![];
        for rowset in &selected_rowsets {
            let dvs = snapshot
                .get_dvs_of(table.table_id(), rowset.rowset_id())
                .map(|dvs| {
                    dvs.iter()
                        .map(|dv_id| self.storage.version.get_dv(table.table_id(), *dv_id))
                        .collect_vec()
                })
                .unwrap_or_default();

            iters.push(
                rowset
                    .iter(column_refs.clone(), dvs, ColumnSeekPosition::start())
                    .await,
            );
        }

        let rowset_id = table.generate_rowset_id();
        let directory = table.get_rowset_path(rowset_id);

        let sort_key = find_sort_key_id(&table.columns);
        let mut iter: SecondaryIterator = if let Some(sort_key) = sort_key {
            MergeIterator::new(
                iters.into_iter().map(|iter| iter.into()).collect_vec(),
                sort_key,
            )
            .into()
        } else {
            ConcatIterator::new(iters).into()
        };

        tokio::fs::create_dir(&directory).await.unwrap();

        let mut builder = RowsetBuilder::new(
            table.columns.clone(),
            &directory,
            ColumnBuilderOptions::from_storage_options(&table.storage_options),
        );

        while let Some(batch) = iter.next_batch(None).await {
            builder.append(batch.to_data_chunk());
        }

        builder.finish_and_flush().await?;

        let rowset = DiskRowset::open(
            directory,
            table.columns.clone(),
            self.storage.block_cache.clone(),
            rowset_id,
            self.storage.options.io_backend,
        )
        .await?;

        // Add RowSets
        let add_rowset_op = EpochOp::AddRowSet((
            AddRowSetEntry {
                rowset_id: rowset.rowset_id(),
                table_id: table.table_ref_id,
            },
            rowset,
        ));

        let mut changes = vec![add_rowset_op];

        // Remove old RowSets
        // and TODO: remove old DVs
        changes.extend(selected_rowsets.iter().map(|x| {
            EpochOp::DeleteRowSet(DeleteRowsetEntry {
                rowset_id: x.rowset_id(),
                table_id: table.table_ref_id,
            })
        }));

        self.storage.version.commit_changes(changes).await?;

        info!(
            "compaction complete: {} -> {}",
            selected_rowsets.iter().map(|x| x.rowset_id()).join(","),
            rowset_id
        );

        Ok(())
    }

    pub async fn run(mut self) -> StorageResult<()> {
        loop {
            {
                let tables = self.storage.tables.read().clone();
                let (epoch, snapshot) = self.storage.version.pin();
                for (_, table) in tables {
                    if let Some(_guard) = self
                        .storage
                        .txn_mgr
                        .try_lock_for_compaction(table.table_id())
                    {
                        self.compact_table(&*snapshot, table).await.unwrap();
                    }
                }
                match self.stop.try_recv() {
                    Ok(_) => break,
                    Err(tokio::sync::oneshot::error::TryRecvError::Closed) => break,
                    _ => {}
                }
                self.storage.version.unpin(epoch);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }
}
