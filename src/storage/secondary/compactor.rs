use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;

use crate::storage::secondary::column::ColumnSeekPosition;
use crate::storage::secondary::concat_iterator::ConcatIterator;
use crate::storage::secondary::rowset::{DiskRowset, RowsetBuilder};
use crate::storage::secondary::ColumnBuilderOptions;
use crate::storage::{StorageColumnRef, StorageResult};

use super::{SecondaryStorage, SecondaryTable};

/// Manages all compactions happening in the storage engine.
pub struct Compactor {
    storage: Arc<SecondaryStorage>,
}

impl Compactor {
    pub fn new(storage: Arc<SecondaryStorage>) -> Self {
        Self { storage }
    }

    async fn compact_table(&self, table: SecondaryTable) -> StorageResult<()> {
        let rowsets = table.inner.read().on_disk.clone();
        let mut selected_rowsets = vec![];
        let mut current_size = 0;
        for (_, rowset) in rowsets {
            let on_disk_size = rowset.on_disk_size();
            if on_disk_size + current_size <= self.storage.options.target_rowset_size as u64 {
                current_size += on_disk_size;
                selected_rowsets.push(rowset);
            }
        }
        if selected_rowsets.len() <= 1 {
            return Ok(());
        }
        let column_refs: Arc<[StorageColumnRef]> = (0..table.shared.columns.len())
            .map(|idx| StorageColumnRef::Idx(idx as u32))
            .collect_vec()
            .into();
        let mut iters = vec![];
        for rowset in &selected_rowsets {
            iters.push(
                rowset
                    .iter(column_refs.clone(), vec![], ColumnSeekPosition::start())
                    .await,
            );
        }
        let mut iter = ConcatIterator::new(iters);
        let rowset_id = table.generate_rowset_id();
        let directory = table.get_rowset_path(rowset_id);
        tokio::fs::create_dir(&directory).await.unwrap();

        let mut builder = RowsetBuilder::new(
            table.shared.columns.clone(),
            &directory,
            ColumnBuilderOptions::from_storage_options(&table.shared.storage_options),
        );

        while let Some(batch) = iter.next_batch(None).await {
            builder.append(batch.to_data_chunk());
        }

        builder.finish_and_flush().await?;

        let rowset = DiskRowset::open(
            directory,
            table.shared.columns.clone(),
            table.shared.block_cache.clone(),
            rowset_id,
        )
        .await?;

        table
            .commit(
                vec![rowset],
                vec![],
                selected_rowsets.iter().map(|x| x.rowset_id()).collect_vec(),
            )
            .await?;

        info!(
            "compaction complete: {} -> {}",
            selected_rowsets.iter().map(|x| x.rowset_id()).join(","),
            rowset_id
        );

        Ok(())
    }

    pub async fn run(self) {
        loop {
            let tables = self.storage.tables.read().clone();
            for (_, table) in tables {
                self.compact_table(table).await.unwrap();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
