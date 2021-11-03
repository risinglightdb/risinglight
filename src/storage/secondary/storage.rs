use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use crate::catalog::RootCatalog;
use crate::storage::secondary::manifest::*;

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

        let mut manifest = Manifest::open(options.path.join("manifest.json")).await?;

        let manifest_ops = manifest.replay().await?;

        let engine = Self {
            catalog: Arc::new(catalog),
            tables: RwLock::new(tables),
            block_cache: Cache::new(options.cache_size),
            manifest: Arc::new(Mutex::new(manifest)),
            options: Arc::new(options),
            next_rowset_id: Arc::new(AtomicU32::new(0)),
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

                    table.apply_add_rowset(disk_rowset)?;
                    rowset_cnt += 1;
                }
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
