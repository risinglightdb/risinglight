// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::DataChunkBuilder;

pub struct SortAggExecutor {
    pub keys: RecExpr,
    pub aggs: RecExpr,
    pub types: Vec<DataType>,
}

impl SortAggExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        let mut last_keys: Option<GroupKeys> = None;
        let mut states = Evaluator::new(&self.aggs).init_agg_states::<Vec<_>>();
        let mut builder = DataChunkBuilder::new(&self.types, PROCESSING_WINDOW_SIZE);

        #[for_await]
        for chunk in child {
            let chunk = chunk?;
            let keys_chunk = Evaluator::new(&self.keys).eval_list(&chunk)?;
            let args_chunk = Evaluator::new(&self.aggs).eval_list(&chunk)?;

            for i in 0..chunk.cardinality() {
                let keys = keys_chunk.row(i);
                if !matches!(&last_keys, Some(last_keys) if keys == last_keys) {
                    if let Some(keys) = last_keys.take() {
                        let results =
                            Evaluator::new(&self.aggs).agg_list_take_result(states.drain(..));
                        if let Some(chunk) = builder.push_row(keys.into_iter().chain(results)) {
                            yield chunk;
                        }
                    }
                    last_keys = Some(keys.values().collect());
                    states = Evaluator::new(&self.aggs).init_agg_states();
                }
                Evaluator::new(&self.aggs).agg_list_append(&mut states, args_chunk.row(i).values());
            }
        }
        if let Some(keys) = last_keys.take() {
            let results = Evaluator::new(&self.aggs).agg_list_take_result(states);
            if let Some(chunk) = builder.push_row(keys.into_iter().chain(results)) {
                yield chunk;
            } else if let Some(chunk) = builder.take() {
                yield chunk;
            }
        }
    }
}
