// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use iter_chunks::IterChunks;
use smallvec::SmallVec;

use super::*;
use crate::array::DataChunkBuilder;
use crate::types::DataValue;

/// The executor of hash aggregation.
pub struct HashAggExecutor {
    pub aggs: RecExpr,
    pub group_keys: RecExpr,
    pub types: Vec<DataType>,
}

pub type GroupKeys = SmallVec<[DataValue; 4]>;
pub type AggValue = SmallVec<[DataValue; 16]>;

impl HashAggExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        let mut states = HashMap::<GroupKeys, AggValue>::new();

        #[for_await]
        for chunk in child {
            let chunk = chunk?;
            let keys_chunk = Evaluator::new(&self.group_keys).eval_list(&chunk)?;
            let args_chunk = Evaluator::new(&self.aggs).eval_list(&chunk)?;

            for i in 0..chunk.cardinality() {
                let keys = keys_chunk.row(i).values().collect();
                let states = states
                    .entry(keys)
                    .or_insert_with(|| Evaluator::new(&self.aggs).init_agg_states());
                Evaluator::new(&self.aggs).agg_list_append(states, args_chunk.row(i).values());
            }
        }

        let mut batches = IterChunks::chunks(states.into_iter(), PROCESSING_WINDOW_SIZE);
        while let Some(batch) = batches.next() {
            let mut builder = DataChunkBuilder::new(&self.types, PROCESSING_WINDOW_SIZE);
            for (key, aggs) in batch {
                if let Some(chunk) = builder.push_row(aggs.into_iter().chain(key.into_iter())) {
                    yield chunk;
                }
            }
            if let Some(chunk) = builder.take() {
                yield chunk;
            }
        }
    }
}
