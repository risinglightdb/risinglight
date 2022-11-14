// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;

use moka::future::Cache;
use tokio::sync::OwnedMutexGuard;

use super::*;
use crate::catalog::TableRefId;
use crate::storage::Table;

/// A table in Secondary engine.
///
/// As `SecondaryStorage` holds the reference to `SecondaryTable`, we cannot store
/// `Arc<SecondaryStorage>` inside `SecondaryTable`. This sturct only contains necessary information
/// to decode the columns of the table.
#[derive(Clone)]
pub struct SecondaryTable {
    /// Table id
    pub table_ref_id: TableRefId,

    /// All columns (ordered) in table
    pub columns: Arc<[ColumnCatalog]>,

    /// Mapping from [`ColumnId`] to column index in `columns`.
    pub column_map: HashMap<ColumnId, usize>,

    /// Root directory of the storage
    pub storage_options: Arc<StorageOptions>,

    /// `VersionManager` from `Storage`. Note that this should be removed after we have refactored
    /// the storage API to have snapshot interface.
    pub version: Arc<VersionManager>,

    /// Will be removed when we have `snapshot` interface.
    pub txn_mgr: Arc<TransactionManager>,

    /// Block cache of the storage engine. Note that this should be removed after we have
    /// refactored the storage API to have snapshot interface.
    pub block_cache: Cache<BlockCacheKey, Block>,

    /// Next RowSet Id and DV Id of the current storage engine
    next_id: Arc<(AtomicU32, AtomicU64)>,
}

impl SecondaryTable {
    pub fn new(
        storage_options: Arc<StorageOptions>,
        table_ref_id: TableRefId,
        columns: &[ColumnCatalog],
        next_id: Arc<(AtomicU32, AtomicU64)>,
        version: Arc<VersionManager>,
        block_cache: Cache<BlockCacheKey, Block>,
        txn_mgr: Arc<TransactionManager>,
    ) -> Self {
        Self {
            columns: columns.into(),
            column_map: columns
                .iter()
                .enumerate()
                .map(|(idx, col)| (col.id(), idx))
                .collect(),
            table_ref_id,
            storage_options,
            next_id,
            version,
            block_cache,
            txn_mgr,
        }
    }

    pub fn generate_rowset_id(&self) -> u32 {
        self.next_id
            .0
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn generate_dv_id(&self) -> u64 {
        self.next_id
            .1
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn get_rowset_path(&self, rowset_id: u32) -> PathBuf {
        self.storage_options
            .path
            .join(format!("{}_{}", self.table_id(), rowset_id))
    }

    pub fn get_dv_path(&self, rowset_id: u32, dv_id: u64) -> PathBuf {
        self.storage_options
            .path
            .join(format!("dv/{}_{}_{}.dv", self.table_id(), rowset_id, dv_id))
    }

    pub fn table_id(&self) -> u32 {
        self.table_ref_id.table_id
    }

    pub async fn lock_for_deletion(&self) -> OwnedMutexGuard<()> {
        self.txn_mgr.lock_for_deletion(self.table_id()).await
    }
}

impl Table for SecondaryTable {
    type Transaction = SecondaryTransaction;

    fn columns(&self) -> StorageResult<Arc<[ColumnCatalog]>> {
        Ok(self.columns.clone())
    }

    fn table_id(&self) -> TableRefId {
        self.table_ref_id
    }

    async fn write(&self) -> StorageResult<SecondaryTransaction> {
        SecondaryTransaction::start(self, false, false).await
    }

    async fn read(&self) -> StorageResult<SecondaryTransaction> {
        SecondaryTransaction::start(self, true, false).await
    }

    async fn update(&self) -> StorageResult<SecondaryTransaction> {
        SecondaryTransaction::start(self, false, true).await
    }
}
