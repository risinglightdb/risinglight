// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::{DataChunk, DataChunkBuilder};
use crate::types::DataType;

/// The executor of a limit operation.
pub struct LimitExecutor {
    pub child: BoxedExecutor,
    pub offset: usize,
    pub limit: usize,
    pub output_types: Vec<DataType>,
}

impl LimitExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        // the number of rows have been processed
        let mut processed = 0;
        let mut builder = DataChunkBuilder::new(self.output_types.iter(), PROCESSING_WINDOW_SIZE);

        #[for_await]
        for batch in self.child {
            if self.limit == 0 {
                break;
            }
            let batch = batch?;
            let cardinality = batch.cardinality();
            let start = processed.max(self.offset) - processed;
            let end = (processed + cardinality).min(self.offset + self.limit) - processed;
            processed += cardinality;
            if start >= end {
                continue;
            }
            for row in batch.rows().skip(start).take(end - start) {
                if let Some(chunk) = builder.push_row(row.values()) {
                    yield chunk
                }
            }
            if processed >= self.offset + self.limit {
                break;
            }
        }

        if let Some(chunk) = { builder }.take() {
            yield chunk;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use futures::TryStreamExt;
    use test_case::test_case;

    use super::*;
    use crate::array::ArrayImpl;
    use crate::types::DataTypeKind;

    #[test_case(&[(0..6)], 1, 4, &[(1..5)])]
    #[test_case(&[(0..6)], 0, 10, &[(0..6)])]
    #[test_case(&[(0..6)], 10, 0, &[])]
    #[test_case(&[(0..2), (2..4), (4..6)], 1, 4, &[(1..5)])]
    #[test_case(&[(0..2), (2..4), (4..6)], 1, 2, &[(1..3)])]
    #[test_case(&[(0..2), (2..4), (4..6)], 3, 0, &[])]
    #[tokio::test]
    async fn limit(
        inputs: &'static [Range<i32>],
        offset: usize,
        limit: usize,
        outputs: &'static [Range<i32>],
    ) {
        let executor = LimitExecutor {
            child: futures::stream::iter(inputs.iter().map(range_to_chunk).map(Ok)).boxed(),
            offset,
            limit,
            output_types: vec![DataType::new(DataTypeKind::Int(None), false)],
        };
        let actual = executor.execute().try_collect::<Vec<_>>().await.unwrap();
        let outputs = outputs.iter().map(range_to_chunk).collect_vec();
        assert_eq!(actual, outputs);
    }

    fn range_to_chunk(range: &Range<i32>) -> DataChunk {
        [ArrayImpl::new_int32(range.clone().collect())]
            .into_iter()
            .collect()
    }
}
