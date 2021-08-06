use crate::transaction::TxnBehavior;
pub struct ConcurrencyMgr {

}

impl TxnBehavior for ConcurrencyMgr {
    fn on_txn_commit(&mut self) {

    }

    fn on_txn_rollback(&mut self) {
        
    }
}