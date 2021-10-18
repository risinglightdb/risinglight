use super::*;

/// A dummy executor that produces a single value.
pub struct DummyScanExecutor;

impl DummyScanExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            yield DataChunk::builder().cardinality(1).build();
        }
    }
}
