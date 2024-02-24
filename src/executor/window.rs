// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::DataChunkBuilder;

/// The executor of window functions.
pub struct WindowExecutor {
    /// A list of over window functions.
    ///
    /// e.g. `(list (over (lag #0) list list))`
    pub exprs: RecExpr,
    /// The types of window function columns.
    pub types: Vec<DataType>,
}

impl WindowExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        let mut states = Evaluator::new(&self.exprs).init_agg_states::<Vec<_>>();

        #[for_await]
        for chunk in child {
            let chunk = chunk?;
            let mut builder = DataChunkBuilder::new(&self.types, chunk.cardinality() + 1);
            for i in 0..chunk.cardinality() {
                Evaluator::new(&self.exprs).agg_list_append(&mut states, chunk.row(i).values());
                let results = Evaluator::new(&self.exprs).agg_list_get_result(&states);
                _ = builder.push_row(results);
            }
            let window_chunk = builder.take().unwrap();
            yield chunk.row_concat(window_chunk);
        }
    }
}
