pub mod txn_manager;
pub mod recovery_mgr;
pub mod buffer_mgr;
pub mod concurrency_mgr;
pub mod txn;
pub mod lock_table;

pub trait TxnBehavior {
    fn on_txn_commit(&mut self);
    fn on_txn_rollback(&mut self);
}