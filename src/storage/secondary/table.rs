use super::*;
use crate::array::DataChunkRef;
use crate::catalog::{ColumnDesc, TableRefId};
use crate::storage::Table;
use async_trait::async_trait;
use itertools::Itertools;
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
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
    /// All on-disk rowsets. As we donot have on-disk structures implemented,
    /// we simply leave a [`DataChunk`] here.
    on_disk: Vec<Arc<DiskRowset>>,

    /// Store info again in inner so that inner struct could access it.
    info: Arc<SecondaryTableInfo>,

    next_rowset_id: AtomicUsize,
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
}

pub(super) type SecondaryTableInnerRef = Arc<RwLock<SecondaryTableInner>>;

impl SecondaryTableInner {
    pub fn new(info: Arc<SecondaryTableInfo>) -> Self {
        Self {
            on_disk: vec![],
            info,
            next_rowset_id: AtomicUsize::new(0),
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
        });

        Self {
            info: info.clone(),
            inner: Arc::new(RwLock::new(SecondaryTableInner::new(info))),
        }
    }

    /// Get snapshot of all rowsets inside table.
    pub(super) fn snapshot(&self) -> StorageResult<Vec<DataChunkRef>> {
        Ok(vec![])
    }

    pub(super) fn add_rowset(&self, rowset: DiskRowset) -> StorageResult<()> {
        self.inner.write().on_disk.push(Arc::new(rowset));
        Ok(())
    }

    pub(super) fn generate_rowset_id(&self) -> usize {
        let inner = self.inner.read();
        inner
            .next_rowset_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn get_rowset_path(&self, rowset_id: usize) -> PathBuf {
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
        Ok(SecondaryTransaction::start(self)?)
    }

    async fn read(&self) -> StorageResult<Self::TransactionType> {
        Ok(SecondaryTransaction::start(self)?)
    }

    async fn update(&self) -> StorageResult<Self::TransactionType> {
        Ok(SecondaryTransaction::start(self)?)
    }
}
