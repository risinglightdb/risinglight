// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use async_channel::{unbounded, Receiver, Sender};
use parking_lot::Mutex as PLMutex;

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
    ///
    /// To make things easy, we use a pair of channel to mock a Mutex. All async Mutex requires
    /// some kind of lifetime in their guards, which make the code hard to implement. An item
    /// in channel represents its availability.
    #[allow(clippy::type_complexity)]
    lock_map: PLMutex<HashMap<u32, (Sender<()>, Receiver<()>)>>,
}

pub struct TransactionLock {
    tx: Sender<()>,
}

impl Drop for TransactionLock {
    fn drop(&mut self) {
        self.tx.try_send(()).unwrap();
    }
}

impl TransactionManager {
    fn get_lock_for_table(&self, table: u32) -> (Sender<()>, Receiver<()>) {
        let mut lock_map = self.lock_map.lock();
        lock_map
            .entry(table)
            .or_insert_with(|| {
                let (tx, rx) = unbounded();
                // only one member can get the lock, so we send one `()` message.
                tx.try_send(()).unwrap();
                (tx, rx)
            })
            .clone()
    }

    /// Get a lock for compaction, return immediately
    pub fn try_lock_for_compaction(&self, table: u32) -> Option<TransactionLock> {
        let (tx, rx) = self.get_lock_for_table(table);
        match rx.try_recv() {
            Ok(()) => Some(TransactionLock { tx }),
            Err(_) => None,
        }
    }

    async fn lock(&self, table: u32) -> TransactionLock {
        let (tx, rx) = self.get_lock_for_table(table);
        rx.recv().await.unwrap();
        TransactionLock { tx }
    }

    /// Get a lock for compaction
    pub async fn lock_for_compaction(&self, table: u32) -> TransactionLock {
        self.lock(table).await
    }

    /// Get a lock for deletion
    pub async fn lock_for_deletion(&self, table: u32) -> TransactionLock {
        self.lock(table).await
    }
}
