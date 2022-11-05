// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use iter_chunks::IterChunks;
use smallvec::{smallvec, SmallVec};

use super::*;
use crate::array::{ArrayBuilderImpl, ArrayImpl, DataChunkBuilder};
use crate::types::DataValue;

/// The executor of hash aggregation.
pub struct HashAggExecutor {
    pub aggs: RecExpr,
    pub group_keys: RecExpr,
    pub types: Vec<DataType>,
}

pub type GroupKeys = SmallVec<[DataValue; 16]>;
pub type AggValue = SmallVec<[DataValue; 16]>;

impl HashAggExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        let len = self.aggs.as_ref().last().unwrap().as_list().len();
        let mut states = HashMap::<GroupKeys, AggValue>::new();

        #[for_await]
        for chunk in child {
            let chunk = chunk?;
            let keys_chunk = ExprRef::new(&self.group_keys).eval_list(&chunk)?;

            for i in 0..chunk.cardinality() {
                let keys = keys_chunk.row(i).values().collect();
                let states = states
                    .entry(keys)
                    .or_insert_with(|| smallvec![DataValue::Null; len]);
                ExprRef::new(&self.aggs).eval_agg_list(states, &chunk.slice(i..=i))?;
            }
        }

        let mut batches = IterChunks::chunks(states.into_iter(), PROCESSING_WINDOW_SIZE);
        while let Some(batch) = batches.next() {
            let mut builder = DataChunkBuilder::new(&self.types, PROCESSING_WINDOW_SIZE);
            for (key, aggs) in batch {
                builder.push_row(aggs.into_iter().chain(key.into_iter()));
            }
            if let Some(chunk) = builder.take() {
                yield chunk;
            }
        }
    }
}
