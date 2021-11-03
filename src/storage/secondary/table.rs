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
    /// sharedrmation about this table, shared by all structs
    pub(super) shared: Arc<SecondaryTableshared>,

    /// Inner lock-procted structures
    pub(super) inner: SecondaryTableInnerRef,
}

pub(super) struct SecondaryTableInner {
    /// All on-disk rowsets. In the future, this should be a MVCC hashmap,
    /// so as to reduce the need to clone the `Arc`s.
    on_disk: Vec<Arc<DiskRowset>>,

    /// Store shared again in inner so that inner struct could access it.
    shared: Arc<SecondaryTableshared>,
}

pub(super) struct SecondaryTableshared {
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

    /// Next RowSet Id of the current table
    next_rowset_id: Arc<AtomicU32>,

    /// Manifest file
    manifest: Arc<Mutex<Manifest>>,
}

pub(super) type SecondaryTableInnerRef = Arc<RwLock<SecondaryTableInner>>;

impl SecondaryTableInner {
    pub fn new(shared: Arc<SecondaryTableshared>) -> Self {
        Self {
            on_disk: vec![],
            shared,
        }
    }

    fn column_descs(&self, ids: &[ColumnId]) -> StorageResult<Vec<ColumnDesc>> {
        ids.iter()
            .map(|id| {
                Ok(self.shared.columns[self
                    .shared
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
        next_rowset_id: Arc<AtomicU32>,
        manifest: Arc<Mutex<Manifest>>,
    ) -> Self {
        let shared = Arc::new(SecondaryTableshared {
            columns: columns.into(),
            column_map: columns
                .iter()
                .enumerate()
                .map(|(idx, col)| (col.id(), idx))
                .collect(),
            table_ref_id,
            storage_options,
            block_cache,
            next_rowset_id,
            manifest,
        });

        Self {
            shared: shared.clone(),
            inner: Arc::new(RwLock::new(SecondaryTableInner::new(shared))),
        }
    }

    /// Get snapshot of all rowsets inside table.
    pub(super) fn snapshot(&self) -> StorageResult<Vec<Arc<DiskRowset>>> {
        let inner = self.inner.read();
        Ok(inner.on_disk.clone())
    }

    pub(super) fn apply_add_rowset(&self, rowset: DiskRowset) -> StorageResult<()> {
        self.inner.write().on_disk.push(Arc::new(rowset));
        Ok(())
    }

    pub(super) async fn add_rowset(&self, rowset: DiskRowset) -> StorageResult<()> {
        info!("RowSet flushed: {}", rowset.rowset_id());
        let mut manifest = self.shared.manifest.lock().await;
        manifest
            .append(ManifestOperation::AddRowSet(AddRowSetEntry {
                rowset_id: rowset.rowset_id(),
                table_id: self.shared.table_ref_id,
            }))
            .await?;
        self.apply_add_rowset(rowset)?;
        Ok(())
    }

    pub(super) fn generate_rowset_id(&self) -> u32 {
        self.shared
            .next_rowset_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn get_rowset_path(&self, rowset_id: u32) -> PathBuf {
        self.shared
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
        self.shared.table_ref_id
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
