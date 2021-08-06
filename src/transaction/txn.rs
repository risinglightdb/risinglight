use crate::transaction::TxnBehavior;
use crate::transaction::recovery_mgr::RecoveryMgr;
use crate::transaction::buffer_mgr::BufferMgr;
use crate::transaction::concurrency_mgr::ConcurrencyMgr;

pub struct Transaction {
    txn_id: u64,
    recovery_mgr: RecoveryMgr,
    buffer_mgr: BufferMgr,
    concurrency_mgr: ConcurrencyMgr
}

impl Transaction {

    pub fn new(txn_id: u64) -> Transaction {
        Transaction {
            txn_id: txn_id,
            buffer_mgr: BufferMgr{},
            recovery_mgr: RecoveryMgr{},
            concurrency_mgr: ConcurrencyMgr{}
        }
    }

    pub fn commit(&mut self) {
      
    }

    pub fn rollback(&mut self) {
        
    }

    pub fn get_recovery_mgr(&self) -> &RecoveryMgr {
        &self.recovery_mgr
    }

    pub fn get_buffer_mgr(&self) -> &BufferMgr {
        &self.buffer_mgr
    }

    pub fn get_concurrency_mgr(&self) -> &ConcurrencyMgr {
        &self.concurrency_mgr
    }
}
