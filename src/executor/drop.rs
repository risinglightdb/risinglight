use super::*;
use crate::{binder::Object, physical_plan::PhysicalDrop};

pub struct DropExecutor {
    pub plan: PhysicalDrop,
    pub storage: StorageRef,
}

impl DropExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            match self.plan.object {
                Object::Table(ref_id) => self.storage.drop_table(ref_id)?,
            }
            yield DataChunk::builder().cardinality(1).build();
        }
    }
}
