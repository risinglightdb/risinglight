use super::*;

/// A dummy executor that produces a single value.
pub struct DummyExecutor;

impl Executor for DummyExecutor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError> {
        Ok(DataChunk::single(0))
    }
}
