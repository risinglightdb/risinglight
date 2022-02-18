// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! Secondary storage engine for RisingLight

// public modules and structures
mod txn_iterator;
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
mod version_manager;
use version_manager::*;
mod transaction_manager;
use transaction_manager::*;
mod checksum;
mod statistics;
pub use checksum::*;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;

use moka::future::Cache;
use parking_lot::RwLock;
use tokio::sync::oneshot::Sender;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::info;

use super::{Storage, StorageResult, TracedStorageError};
use crate::catalog::{ColumnCatalog, RootCatalogRef, TableRefId};
use crate::types::{ColumnId, DatabaseId, SchemaId};

/// Secondary storage of RisingLight.
pub struct SecondaryStorage {
    /// Catalog of the database
    catalog: RootCatalogRef,

    /// All tables in the storage engine
    tables: RwLock<HashMap<TableRefId, SecondaryTable>>,

    /// Options of the current engine
    options: Arc<StorageOptions>,

    /// Block cache of the storage engine
    block_cache: Cache<BlockCacheKey, Block>,

    /// Next RowSet Id and DV Id of the current storage engine
    next_id: Arc<(AtomicU32, AtomicU64)>,

    /// Compactor handler used to cancel compactor run
    #[allow(clippy::type_complexity)]
    compactor_handler: Mutex<(Option<Sender<()>>, Option<JoinHandle<()>>)>,

    /// Vacuum handler used to cancel version manager run
    #[allow(clippy::type_complexity)]
    vacuum_handler: Mutex<(
        Option<tokio::sync::mpsc::UnboundedSender<()>>,
        Option<JoinHandle<()>>,
    )>,

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
        let (tx, rx) = tokio::sync::oneshot::channel();
        let storage = self.clone();
        *self.compactor_handler.lock().await = (
            Some(tx),
            Some(tokio::spawn(async move {
                Compactor::new(storage, rx)
                    .run()
                    .await
                    .expect("compactor stopped unexpectedly");
            })),
        );

        let storage = self.clone();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        *self.vacuum_handler.lock().await = (
            Some(tx),
            Some(tokio::spawn(async move {
                storage
                    .version
                    .run(rx)
                    .await
                    .expect("vacuum stopped unexpectedly");
            })),
        );
    }

    pub async fn shutdown(self: &Arc<Self>) -> StorageResult<()> {
        let mut handler = self.compactor_handler.lock().await;
        info!("shutting down compactor");
        handler.0.take().unwrap().send(()).unwrap();
        handler.1.take().unwrap().await.unwrap();

        let mut handler = self.vacuum_handler.lock().await;
        info!("shutting down vacuum");
        handler.0.take().unwrap().send(()).unwrap();
        handler.1.take().unwrap().await.unwrap();

        Ok(())
    }
}

impl Storage for SecondaryStorage {
    type CreateTableResultFuture<'a> = impl Future<Output = StorageResult<()>> + 'a;
    type DropTableResultFuture<'a> = impl Future<Output = StorageResult<()>> + 'a;
    type TransactionType = SecondaryTransaction;
    type TableType = SecondaryTable;

    fn create_table<'a>(
        &'a self,
        database_id: DatabaseId,
        schema_id: SchemaId,
        table_name: &'a str,
        column_descs: &'a [ColumnCatalog],
    ) -> Self::CreateTableResultFuture<'a> {
        async move {
            self.create_table_inner(database_id, schema_id, table_name, column_descs)
                .await
        }
    }

    fn get_table(&self, table_id: TableRefId) -> StorageResult<SecondaryTable> {
        self.get_table_inner(table_id)
    }

    fn drop_table(&self, table_id: TableRefId) -> Self::DropTableResultFuture<'_> {
        async move { self.drop_table_inner(table_id).await }
    }
}
