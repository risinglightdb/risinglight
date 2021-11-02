use super::*;
use crate::catalog::{ColumnDesc, TableRefId};
use crate::storage::Table;
use async_trait::async_trait;
use itertools::Itertools;
use moka::future::Cache;
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::vec::Vec;

/// A table in Secondary engine.
#[derive(Clone)]
pub struct SecondaryTable {
    /// Information about this table, shared by all structs
    pub(super) info: Arc<SecondaryTableInfo>,

    /// Inner lock-procted structures
    pub(super) inner: SecondaryTableInnerRef,
}

pub(super) struct SecondaryTableInner {
    /// All on-disk rowsets. In the future, this should be a MVCC hashmap,
    /// so as to reduce the need to clone the `Arc`s.
    on_disk: Vec<Arc<DiskRowset>>,

    /// Store info again in inner so that inner struct could access it.
    info: Arc<SecondaryTableInfo>,

    next_rowset_id: AtomicU32,
}

pub(super) struct SecondaryTableInfo {
    /// Table id
    pub table_ref_id: TableRefId,

    /// All columns (ordered) in table
    pub columns: Arc<[ColumnCatalog]>,

    /// Mapping from [`ColumnId`] to column index in `columns`.
    pub column_map: HashMap<ColumnId, usize>,

    /// Root directory of the storage
    pub storage_options: Arc<StorageOptions>,

    /// Block cache for the whole storage engine
    pub block_cache: Cache<BlockCacheKey, Block>,
}

pub(super) type SecondaryTableInnerRef = Arc<RwLock<SecondaryTableInner>>;

impl SecondaryTableInner {
    pub fn new(info: Arc<SecondaryTableInfo>) -> Self {
        Self {
            on_disk: vec![],
            info,
            next_rowset_id: AtomicU32::new(0),
        }
    }

    fn column_descs(&self, ids: &[ColumnId]) -> StorageResult<Vec<ColumnDesc>> {
        ids.iter()
            .map(|id| {
                Ok(self.info.columns[self
                    .info
                    .column_map
                    .get(id)
                    .cloned()
                    .ok_or(StorageError::InvalidColumn(*id))?]
                .desc()
                .clone())
            })
            .try_collect()
    }
}

impl SecondaryTable {
    pub fn new(
        storage_options: Arc<StorageOptions>,
        table_ref_id: TableRefId,
        columns: &[ColumnCatalog],
        block_cache: Cache<BlockCacheKey, Block>,
    ) -> Self {
        let info = Arc::new(SecondaryTableInfo {
            columns: columns.into(),
            column_map: columns
                .iter()
                .enumerate()
                .map(|(idx, col)| (col.id(), idx))
                .collect(),
            table_ref_id,
            storage_options,
            block_cache,
        });

        Self {
            info: info.clone(),
            inner: Arc::new(RwLock::new(SecondaryTableInner::new(info))),
        }
    }

    /// Get snapshot of all rowsets inside table.
    pub(super) fn snapshot(&self) -> StorageResult<Vec<Arc<DiskRowset>>> {
        let inner = self.inner.read();
        Ok(inner.on_disk.clone())
    }

    pub(super) fn add_rowset(&self, rowset: DiskRowset) -> StorageResult<()> {
        info!("RowSet flushed: {}", rowset.rowset_id());
        self.inner.write().on_disk.push(Arc::new(rowset));
        Ok(())
    }

    pub(super) fn generate_rowset_id(&self) -> u32 {
        let inner = self.inner.read();
        inner
            .next_rowset_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn get_rowset_path(&self, rowset_id: u32) -> PathBuf {
        self.info
            .storage_options
            .path
            .join(format!("{}_{}", self.table_id().table_id, rowset_id))
    }
}

#[async_trait]
impl Table for SecondaryTable {
    type TransactionType = SecondaryTransaction;

    fn column_descs(&self, ids: &[ColumnId]) -> StorageResult<Vec<ColumnDesc>> {
        let inner = self.inner.read();
        inner.column_descs(ids)
    }

    fn table_id(&self) -> TableRefId {
        self.info.table_ref_id
    }

    async fn write(&self) -> StorageResult<Self::TransactionType> {
        Ok(SecondaryTransaction::start(self, false)?)
    }

    async fn read(&self) -> StorageResult<Self::TransactionType> {
        Ok(SecondaryTransaction::start(self, true)?)
    }

    async fn update(&self) -> StorageResult<Self::TransactionType> {
        Ok(SecondaryTransaction::start(self, false)?)
    }
}
