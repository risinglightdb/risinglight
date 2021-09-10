use crate::array::{DataChunk, DataChunkRef};

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum ExecutorError {
    #[error("failed to initialize the executor")]
    InitializationError,
}

pub enum ExecutionResult {
    Chunk(DataChunkRef),
    Done,
}

pub trait Executor {
    fn init(&mut self) -> Result<(), ExecutorError>;
    fn execute(&mut self, chunk: ExecutionResult) -> Result<ExecutionResult, ExecutorError>;
    fn done(&mut self) -> Result<(), ExecutorError>;
}
