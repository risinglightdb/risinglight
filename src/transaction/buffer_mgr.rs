use crate::transaction::TxnBehavior;

pub struct BufferMgr {

}

impl TxnBehavior for BufferMgr {
    fn on_txn_commit(&mut self) {
        
    }

    fn on_txn_rollback(&mut self) {
        
    }
}