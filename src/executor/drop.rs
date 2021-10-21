use super::*;
use crate::storage::Storage;
use crate::{binder::Object, physical_planner::PhysicalDrop};
use std::sync::Arc;
/// The executor of `drop` statement.
pub struct DropExecutor<S: Storage> {
    pub plan: PhysicalDrop,
    pub storage: Arc<S>,
}

impl<S: Storage> DropExecutor<S> {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            match self.plan.object {
                Object::Table(ref_id) => self.storage.drop_table(ref_id)?,
            }
            yield DataChunk::builder().cardinality(1).build();
        }
    }
}
