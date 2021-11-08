//! Secondary storage engine for RisingLight

// public modules and structures
mod txn_iterator;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
pub use txn_iterator::*;
mod row_handler;
pub use row_handler::*;
mod table;
pub use table::*;
mod transaction;
pub use transaction::*;
mod options;
pub use options::*;

// internal modules and structures
mod delete_vector;
use delete_vector::*;
mod column;
mod storage;
use column::*;
mod block;
use block::*;
mod concat_iterator;
use concat_iterator::*;
mod manifest;
use manifest::*;
mod rowset;
use rowset::*;
mod index;
use index::*;
mod index_builder;
use index_builder::*;
mod encode;
use encode::*;
mod compactor;
use compactor::*;
mod merge_iterator;
use merge_iterator::*;

#[cfg(test)]
mod tests;

use super::{Storage, StorageError, StorageResult};
use crate::catalog::{ColumnCatalog, RootCatalogRef, TableRefId};
use crate::types::{ColumnId, DatabaseId, SchemaId};
use async_trait::async_trait;
use moka::future::Cache;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Secondary storage of RisingLight.
pub struct SecondaryStorage {
    /// Catalog of the database
    /// TODO(chi): persist catalog in Secondary
    catalog: RootCatalogRef,

    /// All tables in the storage engine
    tables: RwLock<HashMap<TableRefId, SecondaryTable>>,

    /// Options of the current engine
    options: Arc<StorageOptions>,

    /// Block cache of the storage engine
    block_cache: Cache<BlockCacheKey, Block>,

    /// Stores all meta operations inside storage engine
    manifest: Arc<Mutex<Manifest>>,

    /// Next RowSet Id of the current storage engine
    next_rowset_id: Arc<AtomicU32>,

    /// Next DV Id of the current storage engine
    next_dv_id: Arc<AtomicU64>,

    /// Compactor handler used to cancel compactor run
    #[allow(clippy::type_complexity)]
    compactor_handler: Mutex<(Option<Sender<()>>, Option<JoinHandle<StorageResult<()>>>)>,
}

impl SecondaryStorage {
    pub async fn open(options: StorageOptions) -> StorageResult<Self> {
        Self::bootstrap(options).await
    }

    pub fn catalog(&self) -> &RootCatalogRef {
        &self.catalog
    }

    pub async fn spawn_compactor(self: &Arc<Self>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.compactor_handler.lock().await = (
            Some(tx),
            Some(tokio::spawn(Compactor::new(self.clone(), rx).run())),
        );
    }

    pub async fn shutdown(self: &Arc<Self>) -> StorageResult<()> {
        let mut handler = self.compactor_handler.lock().await;
        handler.0.take().unwrap().send(()).unwrap();
        handler.1.take().unwrap().await.unwrap()
    }
}

#[async_trait]
impl Storage for SecondaryStorage {
    type TransactionType = SecondaryTransaction;
    type TableType = SecondaryTable;

    async fn create_table(
        &self,
        database_id: DatabaseId,
        schema_id: SchemaId,
        table_name: &str,
        column_descs: &[ColumnCatalog],
    ) -> StorageResult<()> {
        self.create_table_inner(database_id, schema_id, table_name, column_descs)
            .await
    }

    fn get_table(&self, table_id: TableRefId) -> StorageResult<SecondaryTable> {
        self.get_table_inner(table_id)
    }

    async fn drop_table(&self, table_id: TableRefId) -> StorageResult<()> {
        self.drop_table_inner(table_id).await
    }
}
