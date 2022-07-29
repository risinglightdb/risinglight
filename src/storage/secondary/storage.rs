// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;

use moka::future::Cache;
use parking_lot::RwLock;
use tokio::fs;
use tokio::sync::Mutex;
use tracing::info;

use super::{DiskRowset, Manifest, SecondaryStorage, StorageOptions, StorageResult};
use crate::catalog::RootCatalog;
use crate::storage::secondary::manifest::*;
use crate::storage::secondary::transaction_manager::TransactionManager;
use crate::storage::secondary::version_manager::{EpochOp, VersionManager};
use crate::storage::secondary::{DeleteVector, IOBackend};

impl SecondaryStorage {
    pub(super) async fn bootstrap(options: StorageOptions) -> StorageResult<Self> {
        let catalog = RootCatalog::new();
        let tables = HashMap::new();

        if !options.disable_all_disk_operation {
            // create folder if not exist
            if fs::metadata(&options.path).await.is_err() {
                info!("create db directory at {:?}", options.path);
                fs::create_dir(&options.path).await?;
            }

            // create DV folder if not exist
            let dv_directory = options.path.join("dv");
            if fs::metadata(&dv_directory).await.is_err() {
                fs::create_dir(&dv_directory).await?;
            }
        }

        let enable_fsync = !matches!(options.io_backend, IOBackend::InMemory(_));

        let mut manifest = if options.disable_all_disk_operation {
            Manifest::new_mock()
        } else {
            Manifest::open(options.path.join("manifest.json"), enable_fsync).await?
        };

        let manifest_ops = manifest.replay().await?;

        let options = Arc::new(options);

        let engine = Self {
            catalog: Arc::new(catalog),
            tables: RwLock::new(tables),
            block_cache: Cache::new(options.cache_size as u64),
            options: options.clone(),
            next_id: Arc::new((AtomicU32::new(0), AtomicU64::new(0))),
            version: Arc::new(VersionManager::new(manifest, options.clone())),
            compactor_handler: Mutex::new((None, None)),
            vacuum_handler: Mutex::new((None, None)),
            txn_mgr: Arc::new(TransactionManager::default()),
        };

        info!("applying {} manifest entries", manifest_ops.len());

        let mut rowsets_to_open = HashMap::new();
        let mut dvs_to_open = HashMap::new();

        for op in manifest_ops {
            match op {
                ManifestOperation::CreateTable(entry) => {
                    engine.apply_create_table(&entry)?;
                }
                ManifestOperation::DropTable(entry) => {
                    engine.apply_drop_table(&entry)?;
                }
                ManifestOperation::AddRowSet(entry) => {
                    engine
                        .next_id
                        .0
                        .fetch_max(entry.rowset_id + 1, std::sync::atomic::Ordering::SeqCst);

                    rowsets_to_open.insert((entry.table_id.table_id, entry.rowset_id), entry);
                }
                ManifestOperation::DeleteRowSet(entry) => {
                    rowsets_to_open.remove(&(entry.table_id.table_id, entry.rowset_id));
                }
                ManifestOperation::AddDV(entry) => {
                    engine
                        .next_id
                        .1
                        .fetch_max(entry.dv_id + 1, std::sync::atomic::Ordering::SeqCst);

                    dvs_to_open.insert(
                        (entry.table_id.table_id, entry.rowset_id, entry.dv_id),
                        entry,
                    );
                }
                ManifestOperation::DeleteDV(entry) => {
                    dvs_to_open.remove(&(entry.table_id.table_id, entry.rowset_id, entry.dv_id));
                }
                ManifestOperation::Begin | ManifestOperation::End => {}
            }
        }

        info!(
            "{} tables loaded, {} rowset loaded, {} DV loaded",
            engine.tables.read().len(),
            rowsets_to_open.len(),
            dvs_to_open.len()
        );

        let mut changeset = vec![];

        if !options.disable_all_disk_operation {
            // vacuum unused RowSets
            let mut dir = fs::read_dir(&options.path).await?;
            while let Some(entry) = dir.next_entry().await? {
                if entry.path().is_dir() {
                    if let Some((table_id, rowset_id)) =
                        entry.file_name().to_str().unwrap().split_once('_')
                    {
                        if let (Ok(table_id), Ok(rowset_id)) =
                            (table_id.parse::<u32>(), rowset_id.parse::<u32>())
                        {
                            if !rowsets_to_open.contains_key(&(table_id, rowset_id)) {
                                fs::remove_dir_all(entry.path())
                                    .await
                                    .expect("failed to vacuum unused rowsets");
                            }
                        }
                    }
                }
            }
        }

        // TODO: parallel open

        let tables = engine.tables.read().clone();

        for (_, entry) in rowsets_to_open {
            let table = tables.get(&entry.table_id).unwrap();
            let disk_rowset = DiskRowset::open(
                table.get_rowset_path(entry.rowset_id),
                table.columns.clone(),
                engine.block_cache.clone(),
                entry.rowset_id,
                options.io_backend.clone(),
            )
            .await?;
            changeset.push(EpochOp::AddRowSet((entry, disk_rowset)));
        }

        for (_, entry) in dvs_to_open {
            let table = tables.get(&entry.table_id).unwrap();
            let dv = DeleteVector::open(
                entry.dv_id,
                entry.rowset_id,
                table.get_dv_path(entry.rowset_id, entry.dv_id),
            )
            .await?;
            changeset.push(EpochOp::AddDV((entry, dv)));
        }

        engine.version.commit_changes(changeset).await?;

        // TODO: compact manifest entries

        Ok(engine)
    }
}
