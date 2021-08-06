use crate::buffer::buffer_pool::NaiveBufferPool;
use crate::log::log_manager::LogManager;
use crate::transaction::txn::{Transaction};
use crate::transaction::lock_table::{LockTable};
use std::sync::{Arc, Mutex};

struct TxnManager {
    next_txn_id: u64,
    buffer_pool_ptr: Arc<Mutex<NaiveBufferPool>>,
    log_mgr_ptr: Arc<Mutex<LogManager>>,
    lock_table_ptr: Arc<Mutex<LockTable>>
}

impl TxnManager {
    fn new(buffer_pool_ptr: Arc<Mutex<NaiveBufferPool>>,
        log_mgr_ptr: Arc<Mutex<LogManager>>,
        lock_table_ptr: Arc<Mutex<LockTable>>) -> TxnManager {
        TxnManager {
            next_txn_id: 0,
            buffer_pool_ptr: buffer_pool_ptr,
            log_mgr_ptr: log_mgr_ptr,
            lock_table_ptr: lock_table_ptr
        }
    }

    fn new_txn(&mut self) -> Transaction {
        self.next_txn_id += 1;
        Transaction::new(self.next_txn_id - 1)
    }
}
