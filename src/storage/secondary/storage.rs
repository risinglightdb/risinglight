use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;

use crate::catalog::RootCatalog;
use crate::storage::secondary::{manifest::*, DeleteVector};

use super::{DiskRowset, Manifest, SecondaryStorage, StorageOptions, StorageResult};
use moka::future::Cache;
use parking_lot::RwLock;
use tokio::fs;
use tokio::sync::Mutex;

impl SecondaryStorage {
    pub async fn bootstrap(options: StorageOptions) -> StorageResult<Self> {
        let catalog = RootCatalog::new();
        let tables = HashMap::new();

        // create folder if not exist
        if fs::metadata(&options.path).await.is_err() {
            info!("create db directory at {:?}", options.path);
            fs::create_dir(&options.path).await.unwrap();
        }

        // create DV folder if not exist
        let dv_directory = options.path.join("dv");
        if fs::metadata(&dv_directory).await.is_err() {
            fs::create_dir(&dv_directory).await.unwrap();
        }

        let mut manifest = Manifest::open(options.path.join("manifest.json")).await?;

        let manifest_ops = manifest.replay().await?;

        let engine = Self {
            catalog: Arc::new(catalog),
            tables: RwLock::new(tables),
            block_cache: Cache::new(options.cache_size),
            manifest: Arc::new(Mutex::new(manifest)),
            options: Arc::new(options),
            next_rowset_id: Arc::new(AtomicU32::new(0)),
            next_dv_id: Arc::new(AtomicU64::new(0)),
        };

        info!("applying {} manifest entries", manifest_ops.len());

        let mut rowset_cnt = 0;

        for op in manifest_ops {
            match op {
                ManifestOperation::CreateTable(entry) => {
                    engine.apply_create_table(&entry)?;
                }
                ManifestOperation::DropTable(entry) => {
                    engine.apply_drop_table(&entry)?;
                }
                ManifestOperation::AddRowSet(entry) => {
                    let table = {
                        let tables = engine.tables.read();
                        tables.get(&entry.table_id).unwrap().clone()
                    };

                    // TODO: parallel open
                    let disk_rowset = DiskRowset::open(
                        table.get_rowset_path(entry.rowset_id),
                        table.shared.columns.clone(),
                        table.shared.block_cache.clone(),
                        entry.rowset_id,
                    )
                    .await?;

                    // todo: apply in batch instead of one by one
                    table.apply_commit(vec![disk_rowset], vec![])?;
                    engine
                        .next_rowset_id
                        .fetch_max(entry.rowset_id + 1, std::sync::atomic::Ordering::SeqCst);
                    rowset_cnt += 1;
                }
                ManifestOperation::AddDeleteVector(entry) => {
                    let table = {
                        let tables = engine.tables.read();
                        tables.get(&entry.table_id).unwrap().clone()
                    };

                    let dv = DeleteVector::open(
                        entry.dv_id,
                        entry.rowset_id,
                        table.get_dv_path(entry.rowset_id, entry.dv_id),
                    )
                    .await?;
                    engine
                        .next_dv_id
                        .fetch_max(entry.dv_id + 1, std::sync::atomic::Ordering::SeqCst);
                    table.apply_commit(vec![], vec![dv])?;
                }
                ManifestOperation::Begin | ManifestOperation::End => {}
            }
        }

        info!(
            "{} tables loaded, {} rowset loaded",
            engine.tables.read().len(),
            rowset_cnt
        );

        Ok(engine)
    }
}
