// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::DataChunkBuilder;

/// The executor of simple aggregation.
pub struct SimpleAggExecutor {
    /// A list of aggregations.
    ///
    /// e.g. `(list (sum #0) (count #1))`
    pub aggs: RecExpr,
    pub types: Vec<DataType>,
}

impl SimpleAggExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        let mut states = Evaluator::new(&self.aggs).init_agg_states::<Vec<_>>();
        #[for_await]
        for chunk in child {
            let chunk = chunk?;
            Evaluator::new(&self.aggs).eval_agg_list(&mut states, &chunk)?;
        }
        let mut builder = DataChunkBuilder::new(&self.types, 1);
        let results = Evaluator::new(&self.aggs).agg_list_take_result(states);
        yield builder.push_row(results).unwrap();
    }
}
