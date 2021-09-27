use super::*;
use crate::{binder::Object, physical_plan::PhysicalDrop};

pub struct DropExecutor {
    pub plan: PhysicalDrop,
    pub storage: StorageRef,
    pub output: mpsc::Sender<DataChunk>,
}

impl DropExecutor {
    pub async fn execute(self) -> Result<(), ExecutorError> {
        match self.plan.object {
            Object::Table(ref_id) => self.storage.drop_table(ref_id)?,
        }
        Ok(())
    }
}
