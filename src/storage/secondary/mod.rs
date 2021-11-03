//! Secondary storage engine for RisingLight

mod txn_iterator;
use moka::future::Cache;
use tokio::sync::Mutex;
pub use txn_iterator::*;
mod row_handler;
pub use row_handler::*;
mod table;
pub use table::*;
mod transaction;
pub use transaction::*;
mod rowset;
pub use rowset::*;
mod options;
pub use options::*;
mod concat_iterator;
pub use concat_iterator::*;
mod storage;

mod manifest;
pub use manifest::*;

use super::{Storage, StorageError, StorageResult};
use crate::catalog::{ColumnCatalog, RootCatalogRef, TableRefId};
use crate::types::{ColumnId, DatabaseId, SchemaId};
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

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
}

impl SecondaryStorage {
    pub async fn open(options: StorageOptions) -> StorageResult<Self> {
        Self::bootstrap(options).await
    }

    pub fn catalog(&self) -> &RootCatalogRef {
        &self.catalog
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
