use std::sync::Arc;

use super::*;
use crate::binder::Object;
use crate::optimizer::plan_nodes::PhysicalDrop;
use crate::storage::Storage;
/// The executor of `drop` statement.
pub struct DropExecutor<S: Storage> {
    pub plan: PhysicalDrop,
    pub storage: Arc<S>,
}

impl<S: Storage> DropExecutor<S> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        match self.plan.logical().object().clone() {
            Object::Table(ref_id) => self.storage.drop_table(ref_id).await?,
        }
        yield DataChunk::single(0);
    }
}
