// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex as PLMutex;
use tokio::sync::{Mutex, OwnedMutexGuard};

/// Secondary's Transaction Manager.
///
/// The storage engine of RisingLight is designed to provide snapshot isolation (SI) or even SSI.
/// Therefore, we need a txn manager to coordinate this. We plan to implement this in 3 phases:
///
/// * Only allow one deletion and one compaction on one table. Therefore, the system is by nature
///   SI.
/// * Implement concurrent deletion and compaction, and allow lazy detection of conflicts.
/// * Implement true SI write conflict detection.
#[derive(Default)]
pub struct TransactionManager {
    /// A single big lock for each table
    lock_map: PLMutex<HashMap<u32, Arc<Mutex<()>>>>,
}

impl TransactionManager {
    fn get_lock_for_table(&self, table: u32) -> Arc<Mutex<()>> {
        let mut lock_map = self.lock_map.lock();
        lock_map
            .entry(table)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Get a lock for compaction, return immediately
    pub fn try_lock_for_compaction(&self, table: u32) -> Option<OwnedMutexGuard<()>> {
        let mutex = self.get_lock_for_table(table);
        if let Ok(guard) = mutex.try_lock_owned() {
            Some(guard)
        } else {
            None
        }
    }

    async fn lock(&self, table: u32) -> OwnedMutexGuard<()> {
        let mutex = self.get_lock_for_table(table);
        mutex.lock_owned().await
    }

    /// Get a lock for compaction
    pub async fn lock_for_compaction(&self, table: u32) -> OwnedMutexGuard<()> {
        self.lock(table).await
    }

    /// Get a lock for deletion
    pub async fn lock_for_deletion(&self, table: u32) -> OwnedMutexGuard<()> {
        self.lock(table).await
    }
}
