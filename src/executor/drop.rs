use std::sync::Arc;

use super::*;
use crate::binder::Object;
use crate::logical_optimizer::plan_nodes::PhysicalDrop;
use crate::storage::Storage;
/// The executor of `drop` statement.
pub struct DropExecutor<S: Storage> {
    pub plan: PhysicalDrop,
    pub storage: Arc<S>,
}

impl<S: Storage> DropExecutor<S> {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            match self.plan.object {
                Object::Table(ref_id) => self.storage.drop_table(ref_id).await?,
            }
            yield DataChunk::single(0);
        }
    }
}
