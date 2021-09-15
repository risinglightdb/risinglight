use crate::storage::StorageRef;
use std::sync::Arc;

pub type GlobalEnvRef = Arc<GlobalEnv>;

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone)]
pub struct GlobalEnv {
    pub storage: StorageRef,
}
