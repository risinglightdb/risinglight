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
                if processed >= self.offset + self.limit {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ArrayImpl;
    use futures::TryStreamExt;
    use std::ops::Range;
    use test_case::test_case;

    #[test_case(&[(0..6)], 1, 4, &[(1..5)])]
    #[test_case(&[(0..6)], 0, 10, &[(0..6)])]
    #[test_case(&[(0..6)], 10, 0, &[])]
    #[test_case(&[(0..2), (2..4), (4..6)], 1, 4, &[(1..2), (2..4), (4..5)])]
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
        };
        let actual = executor.execute().try_collect::<Vec<_>>().await.unwrap();
        let outputs = outputs.iter().map(range_to_chunk).collect::<Vec<_>>();
        assert_eq!(actual, outputs);
    }

    fn range_to_chunk(range: &Range<i32>) -> DataChunk {
        [ArrayImpl::Int32(range.clone().collect())]
            .into_iter()
            .collect()
    }
}
