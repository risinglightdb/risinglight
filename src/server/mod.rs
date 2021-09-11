use crate::storage::StorageManagerRef;
use std::sync::Arc;

pub type GlobalEnv = Arc<GlobalVariables>;
#[derive(Clone)]
pub struct GlobalVariables {
    pub storage_mgr_ref: StorageManagerRef,
}
