// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::lock::Mutex;
use parking_lot::Mutex as PLMutex;
use tokio::select;
use tracing::{info, warn};

use super::manifest::*;
use super::{DeleteVector, DiskRowset, StorageOptions, StorageResult};

/// The operations sent to the version manager. Compared with manifest entries, operations
/// like `AddRowSet` needs to be associated with a `DiskRowSet` struct.
pub enum EpochOp {
    CreateTable(CreateTableEntry),
    DropTable(DropTableEntry),
    AddRowSet((AddRowSetEntry, DiskRowset)),
    DeleteRowSet(DeleteRowsetEntry),
    AddDV((AddDVEntry, DeleteVector)),
    DeleteDV(DeleteDVEntry),
}

impl std::fmt::Debug for EpochOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateTable(e) => f.debug_tuple("EpochOp::CreateTable").field(e).finish(),
            Self::DropTable(e) => f.debug_tuple("EpochOp::DropTable").field(e).finish(),
            Self::AddRowSet((e, _)) => f.debug_tuple("EpochOp::AddRowSet").field(e).finish(),
            Self::DeleteRowSet(e) => f.debug_tuple("EpochOp::DeleteRowSet").field(e).finish(),
            Self::AddDV((e, _)) => f.debug_tuple("EpochOp::AddDV").field(e).finish(),
            Self::DeleteDV(e) => f.debug_tuple("EpochOp::DeleteDV").field(e).finish(),
        }
    }
}

/// We store the full information of a snapshot in one `Snapshot` object. In the future, we should
/// implement a MVCC structure for this.
#[derive(Clone, Default)]
pub struct Snapshot {
    /// RowSet IDs in this snapshot. We **only store ID** in snapshot, we need to get the actual
    /// objects from version manager later.
    rowsets: HashMap<u32, HashSet<u32>>,

    /// DVs in this snapshot.
    dvs: HashMap<u32, HashMap<u32, HashSet<u64>>>,
}

impl Snapshot {
    pub fn add_rowset(&mut self, table_id: u32, rowset_id: u32) {
        self.rowsets.entry(table_id).or_default().insert(rowset_id);
    }

    pub fn delete_rowset(&mut self, table_id: u32, rowset_id: u32) {
        let table = self.rowsets.get_mut(&table_id).unwrap();
        table.remove(&rowset_id);
        if table.is_empty() {
            self.rowsets.remove(&table_id);
        }
    }

    pub fn add_dv(&mut self, table_id: u32, rowset_id: u32, dv_id: u64) {
        self.dvs
            .entry(table_id)
            .or_default()
            .entry(rowset_id)
            .or_default()
            .insert(dv_id);
    }

    pub fn delete_dv(&mut self, table_id: u32, rowset_id: u32, dv_id: u64) {
        let table = self.dvs.get_mut(&table_id).unwrap();
        let dvs = table.get_mut(&rowset_id).unwrap();
        dvs.remove(&dv_id);
        if dvs.is_empty() {
            table.remove(&rowset_id);
        }
        if table.is_empty() {
            self.dvs.remove(&table_id);
        }
    }

    pub fn get_dvs_of(&self, table_id: u32, rowset_id: u32) -> Option<&HashSet<u64>> {
        if let Some(rowset) = self.dvs.get(&table_id) {
            if let Some(dvs) = rowset.get(&rowset_id) {
                return Some(dvs);
            }
        }
        None
    }

    pub fn get_rowsets_of(&self, table_id: u32) -> Option<&HashSet<u32>> {
        if let Some(rowset) = self.rowsets.get(&table_id) {
            return Some(rowset);
        }
        None
    }
}

#[derive(Default)]
pub struct VersionManagerInner {
    /// To make things easy, we store the full snapshot of each epoch. In the future, we will use a
    /// MVCC structure for this, and only record changes compared with last epoch.
    status: HashMap<u64, Arc<Snapshot>>,

    /// (TableId, RowSetId) -> Object mapping
    rowsets: HashMap<(u32, u32), Arc<DiskRowset>>,

    /// (TableId, DVId) -> Object mapping
    dvs: HashMap<(u32, u64), Arc<DeleteVector>>,

    /// Reference count of each epoch.
    ref_cnt: HashMap<u64, usize>,

    /// Deletion to apply in each epoch.
    rowset_deletion_to_apply: HashMap<u64, Vec<(u32, u32)>>,

    /// Current epoch number.
    epoch: u64,
}

/// Manages the state history of the storage engine and vacuum the stale files on disk.
///
/// Generally, when a transaction starts, it will take a snapshot and store the state of the
/// merge-tree at the time of starting. As the txn is running, new RowSets are added and old RowSets
/// will no longer be used. So how do we know that we can safely remove a RowSet file?
///
/// [`VersionManager`] manages all RowSets in a multi-version way. Everytime there are some
/// changes in the storage engine, [`VersionManager`] should be notified about this change,
/// and handle out a epoch number for that change. For example,
///
/// * (epoch 0) RowSet 1, 2
/// * (engine) add RowSet 3, remove RowSet 1
/// * (epoch 1) RowSet 2, 3
///
/// Each history state will be associated with an epoch number, which will be used by
/// snapshots. When a snapshot is taken, it will "pin" an epoch number. RowSets logically
/// deleted after that epoch won't be deleted physically until the snapshot "unpins" the
/// epoch number.
///
/// Therefore, [`VersionManager`] is the manifest manager of the whole storage system,
/// which reads and writes manifest, manages all on-disk files and vacuum them when no
/// snapshot holds the corresponding epoch of the file.
///
/// The design choice of separating [`VersionManager`] out of the storage engine is a
/// preparation for a distributed storage engine. In such distributed engine, there will
/// generally be some kind of `MetadataManager` which does all of the things that our
/// [`VersionManager`] do.
pub struct VersionManager {
    /// Inner structure of `VersionManager`. This structure is protected by a parking lot Mutex, so
    /// as to support quick lock and unlock.
    inner: Arc<PLMutex<VersionManagerInner>>,

    /// Manifest file. We only allow one thread to commit changes, and `commit_changes` will hold
    /// this lock until complete. As the commit procedure involves async waiting, we need to use an
    /// async lock.
    manifest: Mutex<Manifest>,

    /// Notify the vacuum to apply changes from one epoch.
    tx: tokio::sync::mpsc::UnboundedSender<()>,

    /// Receiver of the vacuum.
    rx: PLMutex<Option<tokio::sync::mpsc::UnboundedReceiver<()>>>,

    /// Storage options
    storage_options: Arc<StorageOptions>,
}

impl VersionManager {
    pub fn new(manifest: Manifest, storage_options: Arc<StorageOptions>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            manifest: Mutex::new(manifest),
            inner: Arc::new(PLMutex::new(VersionManagerInner::default())),
            tx,
            rx: PLMutex::new(Some(rx)),
            storage_options,
        }
    }

    /// Commit changes and return a new epoch number
    pub async fn commit_changes(&self, ops: Vec<EpochOp>) -> StorageResult<u64> {
        // Hold the manifest lock so that no one else could commit changes.
        let mut manifest = self.manifest.lock().await;

        let mut snapshot;
        let mut entries;
        let current_epoch;
        let mut rowset_deletion_to_apply = vec![];

        {
            // Hold the inner lock, so as to apply the changes to the current status, and add new
            // RowSet and DVs to the pool. This lock is released before persisting entries to
            // manifest.
            let mut inner = self.inner.lock();

            // Save the current epoch for later integrity check.
            current_epoch = inner.epoch;

            // Get snapshot of latest version.
            snapshot = inner
                .status
                .get(&current_epoch)
                .map(|x| x.as_ref().clone())
                .unwrap_or_default();

            // Store entries to be committed into the manifest
            entries = Vec::with_capacity(ops.len());

            for op in ops {
                match op {
                    // For catalog operations, just leave it as-is. The version manager currently
                    // doesn't create MVCC map for catalog operations, and
                    // doesn't not provide interface to access them.
                    EpochOp::CreateTable(entry) => {
                        entries.push(ManifestOperation::CreateTable(entry))
                    }
                    EpochOp::DropTable(entry) => entries.push(ManifestOperation::DropTable(entry)),

                    // For other operations, maintain the snapshot in version manager
                    EpochOp::AddRowSet((entry, rowset)) => {
                        // record the rowset into the pool
                        inner
                            .rowsets
                            .insert((entry.table_id.table_id, entry.rowset_id), Arc::new(rowset));
                        // update the snapshot
                        snapshot.add_rowset(entry.table_id.table_id, entry.rowset_id);
                        entries.push(ManifestOperation::AddRowSet(entry));
                    }
                    EpochOp::DeleteRowSet(entry) => {
                        rowset_deletion_to_apply.push((entry.table_id.table_id, entry.rowset_id));
                        snapshot.delete_rowset(entry.table_id.table_id, entry.rowset_id);
                        entries.push(ManifestOperation::DeleteRowSet(entry));
                    }
                    EpochOp::AddDV((entry, dv)) => {
                        // record the DV into the pool
                        inner
                            .dvs
                            .insert((entry.table_id.table_id, entry.dv_id), Arc::new(dv));
                        // update the snapshot
                        snapshot.add_dv(entry.table_id.table_id, entry.rowset_id, entry.dv_id);
                        entries.push(ManifestOperation::AddDV(entry));
                    }
                    EpochOp::DeleteDV(entry) => {
                        // TODO: record delete op and apply it later
                        snapshot.delete_dv(entry.table_id.table_id, entry.rowset_id, entry.dv_id);
                        entries.push(ManifestOperation::DeleteDV(entry));
                    }
                }
            }
        }

        // Persist the change onto the disk.
        manifest.append(&entries).await?;

        // Add epoch number and make the modified snapshot available.
        let mut inner = self.inner.lock();
        assert_eq!(inner.epoch, current_epoch);
        inner.epoch += 1;
        let epoch = inner.epoch;
        inner.status.insert(epoch, Arc::new(snapshot));
        inner
            .rowset_deletion_to_apply
            .insert(epoch, rowset_deletion_to_apply);

        Ok(epoch)
    }

    /// Pin a snapshot of one epoch, so that all files at this epoch won't be deleted.
    pub fn pin(&self) -> Arc<Version> {
        let mut inner = self.inner.lock();
        let epoch = inner.epoch;
        *inner.ref_cnt.entry(epoch).or_default() += 1;
        Arc::new(Version {
            epoch,
            snapshot: inner.status.get(&epoch).unwrap().clone(),
            inner: self.inner.clone(),
            tx: self.tx.clone(),
        })
    }

    pub fn get_rowset(&self, table_id: u32, rowset_id: u32) -> Arc<DiskRowset> {
        let inner = self.inner.lock();
        inner.rowsets.get(&(table_id, rowset_id)).unwrap().clone()
    }

    pub fn get_dv(&self, table_id: u32, dv_id: u64) -> Arc<DeleteVector> {
        let inner = self.inner.lock();
        inner.dvs.get(&(table_id, dv_id)).unwrap().clone()
    }

    pub async fn find_vacuum(self: &Arc<Self>) -> StorageResult<Vec<(u32, u32)>> {
        let mut inner = self.inner.lock();
        let min_pinned_epoch = inner.ref_cnt.keys().min().cloned();

        // If there is no pinned epoch, all deletions can be applied.
        let vacuum_epoch = min_pinned_epoch.unwrap_or(inner.epoch);

        let can_apply = |epoch, vacuum_epoch| epoch <= vacuum_epoch;

        // Fetch to-be-applied deletions.
        let mut deletions = vec![];
        for (epoch, deletion) in &inner.rowset_deletion_to_apply {
            if can_apply(*epoch, vacuum_epoch) {
                deletions.extend(deletion.iter().cloned());
            }
        }
        inner
            .rowset_deletion_to_apply
            .retain(|k, _| !can_apply(*k, vacuum_epoch));
        for deletion in &deletions {
            if let Some(rowset) = inner.rowsets.remove(deletion) {
                match Arc::try_unwrap(rowset) {
                    Ok(rowset) => drop(rowset),
                    Err(_) => panic!("rowset {:?} is still being used", deletion),
                }
            } else {
                warn!("duplicated deletion dectected, but we can't solve this issue for now -- see https://github.com/risinglightdb/risinglight/issues/566 for more information.");
            }
        }
        Ok(deletions)
    }

    pub async fn do_vacuum(self: &Arc<Self>) -> StorageResult<()> {
        let deletions = self.find_vacuum().await?;

        for (table_id, rowset_id) in deletions {
            let path = self
                .storage_options
                .path
                .join(format!("{}_{}", table_id, rowset_id));
            info!("vacuum {}_{}", table_id, rowset_id);
            if !self.storage_options.disable_all_disk_operation {
                tokio::fs::remove_dir_all(path).await?;
            }
        }

        Ok(())
    }

    pub async fn run(
        self: &Arc<Self>,
        mut stop: tokio::sync::mpsc::UnboundedReceiver<()>,
    ) -> StorageResult<()> {
        let mut vacuum_notifier = self.rx.lock().take().unwrap();
        loop {
            select! {
                Some(_) = vacuum_notifier.recv() => self.do_vacuum().await?,
                Some(_) = stop.recv() => break
            }
        }
        Ok(())
    }
}

pub struct Version {
    pub epoch: u64,
    pub snapshot: Arc<Snapshot>,
    inner: Arc<PLMutex<VersionManagerInner>>,
    tx: tokio::sync::mpsc::UnboundedSender<()>,
}

impl Drop for Version {
    /// Unpin a snapshot of one epoch. When reference counter becomes 0, files might be vacuumed.
    fn drop(&mut self) {
        let mut inner = self.inner.lock();
        let ref_cnt = inner
            .ref_cnt
            .get_mut(&self.epoch)
            .expect("epoch not registered!");
        *ref_cnt -= 1;
        if *ref_cnt == 0 {
            inner.ref_cnt.remove(&self.epoch).unwrap();

            if self.epoch != inner.epoch {
                // TODO: precisely pass the epoch number that can be vacuum.
                self.tx.send(()).unwrap();
            }
        }
    }
}
