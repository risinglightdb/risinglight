// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use binary_heap_plus::BinaryHeap;

use super::*;
use crate::array::DataChunk;
use crate::binder::BoundOrderBy;

/// The executor of a Top N operation.
pub struct TopNExecutor {
    pub child: BoxedExecutor,
    pub offset: usize,
    pub limit: usize,
    pub comparators: Vec<BoundOrderBy>,
}

impl TopNExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let heap_size = self.offset + self.limit;
        let mut heap = BinaryHeap::with_capacity_by(heap_size, |row1, row2| {
            cmp(row1, row2, &self.comparators)
        });

        // collect all chunks
        let mut chunks = vec![];
        #[for_await]
        for batch in self.child {
            chunks.push(batch?);
        }
        chunks.iter().for_each(|chunk| {
            chunk.rows().for_each(|row| {
                if heap.len() < heap_size {
                    heap.push(row);
                } else {
                    let mut top = heap.peek_mut().unwrap();
                    if cmp(&row, &top, &self.comparators) == Ordering::Less {
                        *top = row;
                    }
                }
            })
        });

        yield DataChunk::from_rows(
            heap.into_sorted_vec()
                .into_iter()
                .skip(self.offset)
                .take(self.limit)
                .collect::<Vec<_>>()
                .as_ref(),
            &chunks[0],
        );
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use futures::TryStreamExt;
    use test_case::test_case;

    use super::*;
    use crate::array::ArrayImpl;
    use crate::binder::{BoundExpr, BoundInputRef};
    use crate::catalog::ColumnCatalog;
    use crate::types::{DataTypeExt, DataTypeKind};

    #[test_case(&[(0..6)], 1, 4, false, &[(1..5)])]
    #[test_case(&[(0..6)], 0, 10, false, &[(0..6)])]
    #[test_case(&[(0..6)], 10, 0, false, &[(0..0)])]
    #[test_case(&[(0..2), (2..4), (4..6)], 1, 4, false, &[(1..5)])]
    #[test_case(&[(0..6)], 1, 4, true, &[(1..5)])]
    #[test_case(&[(0..6)], 0, 10, true, &[(0..6)])]
    #[test_case(&[(0..6)], 10, 0, true, &[(0..0)])]
    #[test_case(&[(0..2), (2..4), (4..6)], 1, 4, true, &[(1..5)])]
    #[tokio::test]
    async fn simple(
        inputs: &'static [Range<i32>],
        offset: usize,
        limit: usize,
        desc: bool,
        outputs: &'static [Range<i32>],
    ) {
        let inputs: Vec<DataChunk> = inputs.iter().map(|r| range_to_chunk(false, r)).collect();

        let (top_n, limit_order): (TopNExecutor, LimitExecutor) = equivalent_executors(
            inputs,
            offset,
            limit,
            vec![ColumnCatalog::new(
                0,
                DataTypeKind::Int(None).not_null().to_column("v1".into()),
            )],
            vec![(0, desc)],
        );

        let actual = top_n.execute().try_collect::<Vec<_>>().await.unwrap();
        let outputs = outputs
            .iter()
            .map(|r| range_to_chunk(desc, r))
            .collect_vec();
        assert_eq!(actual, outputs);

        let outputs_limit_order = limit_order.execute().try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(actual, outputs_limit_order);
    }

    fn range_to_chunk(reverse: bool, range: &Range<i32>) -> DataChunk {
        let array = if reverse {
            range.clone().rev().collect()
        } else {
            range.clone().collect()
        };
        [ArrayImpl::Int32(array)].into_iter().collect()
    }

    fn equivalent_executors(
        inputs: Vec<DataChunk>,
        offset: usize,
        limit: usize,
        catalog: Vec<ColumnCatalog>,
        idx_desc: Vec<(usize, bool)>,
    ) -> (TopNExecutor, LimitExecutor) {
        let comparators = comparators(catalog, idx_desc.as_ref());

        let top_n = TopNExecutor {
            child: futures::stream::iter(inputs.clone().into_iter().map(Ok)).boxed(),
            offset,
            limit,
            comparators: comparators.clone(),
        };

        let limit_order = LimitExecutor {
            child: OrderExecutor {
                child: futures::stream::iter(inputs.into_iter().map(Ok)).boxed(),
                comparators,
            }
            .execute(),
            offset,
            limit,
        };
        (top_n, limit_order)
    }

    fn comparators(catalog: Vec<ColumnCatalog>, idx_desc: &[(usize, bool)]) -> Vec<BoundOrderBy> {
        idx_desc
            .iter()
            .map(|(idx, desc)| BoundOrderBy {
                expr: BoundExpr::InputRef(BoundInputRef {
                    index: *idx,
                    return_type: catalog[*idx].datatype(),
                }),
                descending: *desc,
            })
            .collect()
    }
}
