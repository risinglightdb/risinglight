use super::*;
use crate::catalog::{ColumnDesc, TableRefId};
use crate::storage::Table;
use async_trait::async_trait;
use itertools::Itertools;
use moka::future::Cache;
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64};
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
    pub on_disk: HashMap<u32, Arc<DiskRowset>>,

    /// All deletion vectors of this table.
    pub dv: HashMap<u32, Vec<Arc<DeleteVector>>>,

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

    /// Next DV Id of the current table
    next_dv_id: Arc<AtomicU64>,

    /// Manifest file
    manifest: Arc<Mutex<Manifest>>,
}

pub(super) type SecondaryTableInnerRef = Arc<RwLock<SecondaryTableInner>>;

impl SecondaryTableInner {
    pub fn new(shared: Arc<SecondaryTableshared>) -> Self {
        Self {
            on_disk: HashMap::new(),
            dv: HashMap::new(),
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

    fn apply_add_rowset(&mut self, rowset: DiskRowset) -> StorageResult<()> {
        self.on_disk.insert(rowset.rowset_id(), Arc::new(rowset));
        Ok(())
    }

    fn apply_add_dv(&mut self, dv: DeleteVector) -> StorageResult<()> {
        self.dv
            .entry(dv.rowset_id())
            .or_insert_with(Vec::new)
            .push(Arc::new(dv));
        Ok(())
    }

    fn apply_delete_rowset(&mut self, rowset: u32) -> StorageResult<()> {
        self.on_disk.remove(&rowset).unwrap();
        Ok(())
    }
}

impl SecondaryTable {
    pub fn new(
        storage_options: Arc<StorageOptions>,
        table_ref_id: TableRefId,
        columns: &[ColumnCatalog],
        block_cache: Cache<BlockCacheKey, Block>,
        next_rowset_id: Arc<AtomicU32>,
        next_dv_id: Arc<AtomicU64>,
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
            next_dv_id,
            manifest,
        });

        Self {
            shared: shared.clone(),
            inner: Arc::new(RwLock::new(SecondaryTableInner::new(shared))),
        }
    }

    pub(super) fn apply_commit(
        &self,
        rowsets: Vec<DiskRowset>,
        dvs: Vec<DeleteVector>,
        deleted_rowsets: Vec<u32>,
    ) -> StorageResult<()> {
        let mut inner = self.inner.write();
        for rowset in rowsets {
            inner.apply_add_rowset(rowset)?;
        }
        for dv in dvs {
            inner.apply_add_dv(dv)?;
        }
        for rowset in deleted_rowsets {
            inner.apply_delete_rowset(rowset)?;
        }
        Ok(())
    }

    pub(super) async fn commit(
        &self,
        rowsets: Vec<DiskRowset>,
        dvs: Vec<DeleteVector>,
        deleted_rowsets: Vec<u32>,
    ) -> StorageResult<()> {
        info!(
            "RowSet {} flushed, DV {} flushed",
            rowsets
                .iter()
                .map(|x| format!("#{}", x.rowset_id()))
                .join(","),
            dvs.iter()
                .map(|x| format!("#{}(RS{})", x.dv_id(), x.rowset_id()))
                .join(",")
        );
        let mut manifest = self.shared.manifest.lock().await;
        let mut ops = vec![];

        for rowset in &rowsets {
            ops.push(ManifestOperation::AddRowSet(AddRowSetEntry {
                rowset_id: rowset.rowset_id(),
                table_id: self.shared.table_ref_id,
            }));
        }

        for rowset_id in &deleted_rowsets {
            ops.push(ManifestOperation::DeleteRowSet(DeleteRowsetEntry {
                table_id: self.shared.table_ref_id,
                rowset_id: *rowset_id,
            }));
        }

        for dv in &dvs {
            ops.push(ManifestOperation::AddDeleteVector(AddDeleteVectorEntry {
                dv_id: dv.dv_id(),
                table_id: self.shared.table_ref_id,
                rowset_id: dv.rowset_id(),
            }));
        }

        manifest.append(&ops).await?;
        self.apply_commit(rowsets, dvs, deleted_rowsets)?;
        Ok(())
    }

    pub(super) fn generate_rowset_id(&self) -> u32 {
        self.shared
            .next_rowset_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn generate_dv_id(&self) -> u64 {
        self.shared
            .next_dv_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn get_rowset_path(&self, rowset_id: u32) -> PathBuf {
        self.shared
            .storage_options
            .path
            .join(format!("{}_{}", self.table_id().table_id, rowset_id))
    }

    pub(super) fn get_dv_path(&self, rowset_id: u32, dv_id: u64) -> PathBuf {
        self.shared.storage_options.path.join(format!(
            "dv/{}_{}_{}.dv",
            self.table_id().table_id,
            rowset_id,
            dv_id
        ))
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
