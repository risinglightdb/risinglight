// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use iter_chunks::IterChunks;
use smallvec::SmallVec;

use super::*;
use crate::array::{ArrayBuilderImpl, ArrayImpl};
use crate::types::DataValue;
use crate::v1::binder::{BoundAggCall, BoundExpr};
use crate::v1::executor::aggregation::AggregationState;

/// The executor of hash aggregation.
pub struct HashAggExecutor {
    pub agg_calls: Vec<BoundAggCall>,
    pub group_keys: Vec<BoundExpr>,
    pub child: BoxedExecutor,
}

pub type HashKey = SmallVec<[DataValue; 16]>;
pub type HashValue = SmallVec<[Box<dyn AggregationState>; 16]>;

impl HashAggExecutor {
    fn execute_inner(
        state_entries: &mut HashMap<Arc<HashKey>, HashValue>,
        chunk: DataChunk,
        agg_calls: &[BoundAggCall],
        group_keys: &[BoundExpr],
    ) -> Result<(), ExecutorError> {
        // Eval group keys and arguments
        let group_cols: SmallVec<[ArrayImpl; 16]> =
            group_keys.iter().map(|e| e.eval(&chunk)).try_collect()?;
        let arrays: SmallVec<[ArrayImpl; 16]> = agg_calls
            .iter()
            .map(|agg| agg.args[0].eval(&chunk))
            .try_collect()?;

        // Update states
        let num_rows = chunk.cardinality();
        for row_idx in 0..num_rows {
            let mut group_key = HashKey::new();
            for col in group_cols.iter() {
                group_key.push(col.get(row_idx));
            }
            let group_key = Arc::new(group_key);

            if !state_entries.contains_key(&group_key) {
                state_entries.insert(group_key.clone(), create_agg_states(agg_calls));
            }
            // since we just checked existence, the key must exist so we `unwrap` directly
            let states = state_entries.get_mut(&group_key).unwrap();
            for (array, state) in arrays.iter().zip_eq(states.iter_mut()) {
                // TODO: support aggregations with multiple arguments
                state.update_single(&array.get(row_idx))?;
            }
        }

        Ok(())
    }

    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    async fn finish_agg(
        state_entries: HashMap<Arc<HashKey>, HashValue>,
        agg_calls: Vec<BoundAggCall>,
        group_keys: Vec<BoundExpr>,
    ) {
        // We use `iter_chunks::IterChunks` instead of `IterTools::Chunks` here, since
        // the latter doesn't implement Send.
        let mut batches = IterChunks::chunks(state_entries.iter(), PROCESSING_WINDOW_SIZE);
        while let Some(batch) = batches.next() {
            let mut key_builders = group_keys
                .iter()
                .map(|e| ArrayBuilderImpl::new(&e.return_type()))
                .collect::<Vec<ArrayBuilderImpl>>();
            let mut res_builders = agg_calls
                .iter()
                .map(|agg| ArrayBuilderImpl::new(&agg.return_type))
                .collect::<Vec<ArrayBuilderImpl>>();
            for (key, val) in batch {
                // Push group key
                for (k, builder) in key.iter().zip_eq(key_builders.iter_mut()) {
                    builder.push(k);
                }
                // Push aggregate result
                for (state, builder) in val.iter().zip_eq(res_builders.iter_mut()) {
                    builder.push(&state.output());
                }
            }
            key_builders.append(&mut res_builders);
            yield key_builders.into_iter().collect()
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let mut state_entries = HashMap::new();

        #[for_await]
        for chunk in self.child {
            let chunk = chunk?;
            Self::execute_inner(&mut state_entries, chunk, &self.agg_calls, &self.group_keys)?;
        }

        #[for_await]
        for chunk in Self::finish_agg(state_entries, self.agg_calls, self.group_keys) {
            let chunk = chunk?;
            yield chunk
        }
    }
}
