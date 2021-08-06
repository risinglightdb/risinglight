use crate::transaction::TxnBehavior;

pub struct RecoveryMgr {
    
}

impl TxnBehavior for RecoveryMgr {
    fn on_txn_commit(&mut self) {

    }

    fn on_txn_rollback(&mut self) {
        
    }
}