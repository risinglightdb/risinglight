// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

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
use tokio::sync::oneshot::Sender;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::info;
pub use transaction::*;
use transaction_manager::*;
pub use txn_iterator::*;
use version_manager::*;

use super::{Storage, StorageResult, TracedStorageError};
use crate::catalog::{ColumnCatalog, ColumnId, DatabaseId, RootCatalogRef, SchemaId, TableRefId};

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

#[cfg(test)]
mod tests;

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
            Some(
                tokio::task::Builder::default()
                    .name("compactor")
                    .spawn(async move {
                        Compactor::new(storage, rx)
                            .run()
                            .await
                            .expect("compactor stopped unexpectedly");
                    })
                    .expect("failed to spawn task"),
            ),
        );

        let storage = self.clone();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        *self.vacuum_handler.lock().await = (
            Some(tx),
            Some(
                tokio::task::Builder::default()
                    .name("vacuum")
                    .spawn(async move {
                        storage
                            .version
                            .run(rx)
                            .await
                            .expect("vacuum stopped unexpectedly");
                    })
                    .expect("failed to spawn task"),
            ),
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
    type Transaction = SecondaryTransaction;
    type Table = SecondaryTable;

    async fn create_table(
        &self,
        database_id: DatabaseId,
        schema_id: SchemaId,
        table_name: &str,
        column_descs: &[ColumnCatalog],
        ordered_pk_ids: &[ColumnId],
    ) -> StorageResult<()> {
        self.create_table_inner(
            database_id,
            schema_id,
            table_name,
            column_descs,
            ordered_pk_ids,
        )
        .await
    }

    fn get_table(&self, table_id: TableRefId) -> StorageResult<SecondaryTable> {
        self.get_table_inner(table_id)
    }

    async fn drop_table(&self, table_id: TableRefId) -> StorageResult<()> {
        self.drop_table_inner(table_id).await
    }
}
