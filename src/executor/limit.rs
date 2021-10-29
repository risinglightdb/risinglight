use super::*;
use crate::array::DataChunk;

/// The executor of a limit operation.
pub struct LimitExecutor {
    pub child: BoxedExecutor,
    pub offset: usize,
    pub limit: usize,
}

impl LimitExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            // the number of rows have been processed
            let mut processed = 0;

            for await batch in self.child {
                let batch = batch?;
                let cardinality = batch.cardinality();
                let start = processed.max(self.offset) - processed;
                let end = (processed + cardinality).min(self.offset + self.limit) - processed;
                processed += cardinality;
                if start >= end {
                    continue;
                }
                if (start..end) == (0..cardinality) {
                    yield batch;
                } else {
                    yield batch.slice(start..end);
                }
                if processed >= self.limit {
                    return;
                }
            }
        }
    }
}
