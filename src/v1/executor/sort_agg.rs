// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.
#![allow(dead_code)]
use smallvec::SmallVec;

use super::*;
use crate::array::{ArrayBuilderImpl, ArrayImpl};
use crate::v1::binder::BoundAggCall;

pub struct SortAggExecutor {
    pub agg_calls: Vec<BoundAggCall>,
    pub group_keys: Vec<BoundExpr>,
    pub child: BoxedExecutor,
}

impl SortAggExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let mut last_key = None::<HashKey>;
        let mut states = create_agg_states(&self.agg_calls);

        #[for_await]
        for chunk in self.child {
            // Eval group keys and arguments
            let chunk = chunk?;
            let exprs: SmallVec<[ArrayImpl; 16]> = self
                .agg_calls
                .iter()
                .map(|agg| agg.args[0].eval(&chunk))
                .try_collect()?;
            let group_cols: SmallVec<[ArrayImpl; 16]> = self
                .group_keys
                .iter()
                .map(|e| e.eval(&chunk))
                .try_collect()?;

            let num_rows = chunk.cardinality();
            for row_idx in 0..num_rows {
                // Create group key
                let mut group_key = HashKey::new();
                for col in group_cols.iter() {
                    group_key.push(col.get(row_idx));
                }
                // Check group key & last key
                if let Some(last_key) = last_key {
                    if last_key != group_key {
                        yield Self::finish_agg(&states);
                        states = create_agg_states(&self.agg_calls);
                    }
                }
                for (state, expr) in states.iter_mut().zip_eq(&exprs) {
                    state.update_single(&expr.get(row_idx))?;
                }
                last_key = Some(group_key);
            }
        }
        if last_key.is_some() {
            yield Self::finish_agg(&states);
        }
    }

    fn finish_agg(states: &SmallVec<[Box<dyn AggregationState>; 16]>) -> DataChunk {
        return states
            .iter()
            .map(|s| {
                let result = &s.output();
                let mut builder = ArrayBuilderImpl::with_capacity(1, &result.data_type());
                builder.push(result);
                builder.finish()
            })
            .collect::<DataChunk>();
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::array::ArrayImpl;
    use crate::types::{DataType, DataTypeKind};
    use crate::v1::binder::{AggKind, BoundInputRef};

    #[tokio::test]
    async fn test_no_rows() {
        test_group_agg(vec![0, 1], vec![1, 2], vec![vec![], vec![], vec![]], vec![]).await;
    }

    #[tokio::test]
    async fn test_multi_group_agg() {
        test_group_agg(
            vec![0, 1],
            vec![1, 2],
            vec![
                vec![1.1, 0.2, 0.3, 0.4, 0.5],
                vec![1.1, 1.1, 1.3, 1.4, 1.5],
                vec![1.2, 1.2, 2.3, 2.4, 2.5],
            ],
            vec![
                vec![1.3, 2.2],
                vec![0.3, 1.3],
                vec![0.4, 1.4],
                vec![0.5, 1.5],
            ],
        )
        .await;
        test_group_agg(
            vec![0, 1],
            vec![1, 2],
            vec![
                vec![0.1, 0.2, 0.3, 0.4, 0.5],
                vec![1.1, 1.1, 1.3, 1.4, 1.5],
                vec![1.1, 1.2, 2.3, 2.4, 2.5],
            ],
            vec![
                vec![0.1, 1.1],
                vec![0.2, 1.1],
                vec![0.3, 1.3],
                vec![0.4, 1.4],
                vec![0.5, 1.5],
            ],
        )
        .await
    }

    #[tokio::test]
    async fn test_single_group_agg() {
        test_group_agg(
            vec![0, 1],
            vec![0],
            vec![vec![1.0, 1.0], vec![1.0, 2.0]],
            vec![vec![2.0, 3.0]],
        )
        .await;
        test_group_agg(
            vec![0, 1],
            vec![1],
            vec![
                vec![1.1, 0.2, 0.3, 0.4, 0.5],
                vec![1.1, 1.1, 1.3, 1.4, 1.5],
                vec![2.1, 2.2, 2.3, 2.4, 2.5],
            ],
            vec![
                vec![1.3, 2.2],
                vec![0.3, 1.3],
                vec![0.4, 1.4],
                vec![0.5, 1.5],
            ],
        )
        .await;
        test_group_agg(
            vec![0, 1],
            vec![1],
            vec![
                vec![0.1, 0.2, 0.3, 0.4, 0.5],
                vec![1.1, 1.2, 1.3, 1.4, 1.5],
                vec![2.1, 2.2, 2.3, 2.4, 2.5],
            ],
            vec![
                vec![0.1, 1.1],
                vec![0.2, 1.2],
                vec![0.3, 1.3],
                vec![0.4, 1.4],
                vec![0.5, 1.5],
            ],
        )
        .await
    }

    async fn test_group_agg(
        agg_call_index: Vec<usize>,
        group_key_index: Vec<usize>,
        cols: Vec<Vec<f64>>,
        expected_cols: Vec<Vec<f64>>,
    ) {
        let mut agg_calls = vec![];
        for index in agg_call_index {
            agg_calls.push(create_sum_agg_call(index));
        }

        let mut group_keys = vec![];
        for index in group_key_index {
            group_keys.push(create_input_ref(index));
        }

        let child: BoxedExecutor = async_stream::try_stream! {
            let mut child = vec![];
            for col in cols {
                child.push(ArrayImpl::new_float64(col.into_iter().collect()));
            }
            yield child.into_iter().collect()
        }
        .boxed();

        let executor = SortAggExecutor {
            agg_calls,
            group_keys,
            child,
        };
        let mut executor = executor.execute();

        let mut expected_cols_size = 0;
        while let Some(chunk) = executor.next().await {
            let chunk = chunk.unwrap();
            let expected_col = expected_cols.get(expected_cols_size).unwrap();
            let mut expected_array = vec![];
            for data in expected_col {
                expected_array.push(ArrayImpl::new_float64(vec![*data].into_iter().collect()));
            }
            assert_eq!(chunk.arrays(), expected_array);
            expected_cols_size += 1;
        }
        assert_eq!(expected_cols_size, expected_cols.len());
    }

    fn create_sum_agg_call(value: usize) -> BoundAggCall {
        BoundAggCall {
            kind: AggKind::Sum,
            args: vec![BoundExpr::InputRef(BoundInputRef {
                index: value,
                return_type: DataType::new(DataTypeKind::Decimal(Some(15), Some(2)), false),
            })],
            return_type: DataType::new(DataTypeKind::Float64, false),
        }
    }

    fn create_input_ref(value: usize) -> BoundExpr {
        BoundExpr::InputRef(BoundInputRef {
            index: value,
            return_type: DataType::new(DataTypeKind::Int32, false),
        })
    }

    fn create_expected_col(cols: Vec<f64>) -> Vec<ArrayImpl> {
        let mut expected = Vec::new();
        for col in cols {
            expected.push(ArrayImpl::new_float64([col].into_iter().collect()));
        }
        expected
    }
}
