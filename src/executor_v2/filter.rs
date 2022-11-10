// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::{Array, ArrayImpl, DataChunk};

/// The executor of a filter operation.
pub struct FilterExecutor {
    pub condition: RecExpr,
}

impl FilterExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        #[for_await]
        for batch in child {
            let batch = batch?;
            let vis = match Evaluator::new(&self.condition).eval(&batch)? {
                ArrayImpl::Bool(a) => a,
                _ => panic!("filters can only accept bool array"),
            };
            yield batch.filter(vis.iter().map(|b| matches!(b, Some(true))));
        }
    }
}
