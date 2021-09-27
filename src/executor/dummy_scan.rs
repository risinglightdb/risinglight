use super::*;
use crate::array::DataChunk;

pub struct DummyScanExecutor {
    pub output: mpsc::Sender<DataChunk>,
}

impl DummyScanExecutor {
    pub async fn execute(self) -> Result<(), ExecutorError> {
        let result = DataChunk::builder().cardinality(1).build();
        self.output.send(result).await.ok().unwrap();
        Ok(())
    }
}
