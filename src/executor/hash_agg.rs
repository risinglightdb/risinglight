// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use ahash::{HashMap, HashMapExt};
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
        let mut chunks = Vec::new();

        #[for_await]
        for chunk in child {
            chunks.push(chunk?);
        }

        println!("chunk size {}", chunks.len());
        let thread_num = 40;
        // Split chunk into multiple threads
        let chunk_size = (chunks.len() + thread_num - 1) / thread_num;
        let chunks = chunks
            .chunks(chunk_size)
            .map(|x| x.to_vec())
            .collect::<Vec<_>>();

        let mut handles = Vec::new();
        for chunk in chunks {
            let aggs = self.aggs.clone();
            let group_keys = self.group_keys.clone();
            let handle = tokio::spawn(async move {
                let mut states = HashMap::<GroupKeys, AggValue>::new();
                for chunk in chunk {
                    let keys_chunk = Evaluator::new(&group_keys).eval_list(&chunk).unwrap();
                    let args_chunk = Evaluator::new(&aggs).eval_list(&chunk).unwrap();

                    for i in 0..chunk.cardinality() {
                        let keys = keys_chunk.row(i).values().collect();
                        let states = states
                            .entry(keys)
                            .or_insert_with(|| Evaluator::new(&aggs).init_agg_states());
                        Evaluator::new(&aggs).agg_list_append(states, args_chunk.row(i).values());
                    }
                }
                states
            });
            handles.push(handle);
        }

        let mut states = HashMap::<GroupKeys, AggValue>::new();

        for handle in handles {
            let chunk_states = handle.await.unwrap();
            for (key, aggs) in chunk_states {
                let states = states
                    .entry(key)
                    .or_insert_with(|| Evaluator::new(&self.aggs).init_agg_states());
                Evaluator::new(&self.aggs).agg_list_append(states, aggs.into_iter());
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
