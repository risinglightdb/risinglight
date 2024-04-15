// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! Secondary storage engine for RisingLight

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;

use block::*;
pub use checksum::*;
use column::*;
use compactor::*;
use concat_iterator::*;
use delete_vector::*;
use encode::*;
use index::*;
use index_builder::*;
use manifest::*;
use merge_iterator::*;
use moka::future::Cache;
pub use options::*;
use parking_lot::RwLock;
pub use row_handler::*;
use rowset::*;
pub use table::*;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::info;
pub use transaction::*;
use transaction_manager::*;
pub use txn_iterator::*;
use version_manager::*;

use super::{Storage, StorageResult, TableRef, TracedStorageError};
use crate::catalog::{ColumnCatalog, ColumnId, RootCatalogRef, SchemaId, TableRefId};

// public modules and structures
mod options;
mod row_handler;
mod table;
mod transaction;
mod txn_iterator;

// internal modules and structures
mod block;
mod checksum;
mod column;
mod compactor;
mod concat_iterator;
mod delete_vector;
mod encode;
mod index;
mod index_builder;
mod manifest;
mod merge_iterator;
mod rowset;
mod statistics;
mod storage;
mod transaction_manager;
mod version_manager;

const MANIFEST_FILE_NAME: &str = "manifest.json";

#[cfg(test)]
mod tests;

/// Disk storage engine.
pub struct SecondaryStorage {
    /// Catalog of the database
    catalog: RootCatalogRef,

    /// All tables in the storage engine
    tables: RwLock<HashMap<TableRefId, Arc<SecondaryTable>>>,

    /// Options of the current engine
    options: Arc<StorageOptions>,

    /// Block cache of the storage engine
    block_cache: Cache<BlockCacheKey, Block>,

    /// Next RowSet Id and DV Id of the current storage engine
    next_id: Arc<(AtomicU32, AtomicU64)>,

    /// Compactor handle used to cancel compactor run
    compactor_handle: Mutex<Option<JoinHandle<()>>>,

    /// Vacuum handle used to cancel version manager run
    vacuum_handle: Mutex<Option<JoinHandle<()>>>,

    /// Manages all history states and vacuum unused files.
    version: Arc<VersionManager>,

    /// Manages all ongoing txns
    txn_mgr: Arc<TransactionManager>,
}

impl SecondaryStorage {
    pub async fn open(options: StorageOptions) -> StorageResult<Self> {
        Self::bootstrap(options).await
    }

    pub fn catalog(&self) -> &RootCatalogRef {
        &self.catalog
    }

    pub async fn spawn_compactor(self: &Arc<Self>) {
        let storage = self.clone();
        *self.compactor_handle.lock().await = Some(
            tokio::task::Builder::default()
                .name("compactor")
                .spawn(async move {
                    Compactor::new(storage).run().await;
                })
                .expect("failed to spawn task"),
        );

        let storage = self.clone();
        *self.vacuum_handle.lock().await = Some(
            tokio::task::Builder::default()
                .name("vacuum")
                .spawn(async move {
                    storage
                        .version
                        .run()
                        .await
                        .expect("vacuum stopped unexpectedly");
                })
                .expect("failed to spawn task"),
        );
    }

    async fn shutdown_inner(&self) -> StorageResult<()> {
        if let Some(handle) = self.compactor_handle.lock().await.take() {
            info!("shutting down compactor");
            handle.abort();
        }
        if let Some(handle) = self.vacuum_handle.lock().await.take() {
            info!("shutting down vacuum");
            handle.abort();
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Storage for SecondaryStorage {
    async fn create_table(
        &self,
        schema_id: SchemaId,
        table_name: &str,
        column_descs: &[ColumnCatalog],
        ordered_pk_ids: &[ColumnId],
    ) -> StorageResult<()> {
        self.create_table_inner(schema_id, table_name, column_descs, ordered_pk_ids)
            .await
    }

    async fn get_table(&self, table_id: TableRefId) -> StorageResult<TableRef> {
        Ok(self.get_table_inner(table_id)?)
    }

    async fn drop_table(&self, table_id: TableRefId) -> StorageResult<()> {
        self.drop_table_inner(table_id).await
    }

    async fn shutdown(&self) -> StorageResult<()> {
        self.shutdown_inner().await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Returns true if the storage engine supports range filter scan.
    fn support_range_filter_scan(&self) -> bool {
        true
    }

    /// Returns true if scanned table is sorted by primary key.
    fn table_is_sorted_by_primary_key(&self) -> bool {
        true
    }
}
