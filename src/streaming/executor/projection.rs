// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::executor::Evaluator;

/// The executor of project operation.
pub struct ProjectionExecutor {
    /// A list of expressions.
    ///
    /// e.g. `(list (+ #0 #1) #0)`
    pub projs: RecExpr,
}

impl ProjectionExecutor {
    #[try_stream(boxed, ok = StreamChunk, error = Error)]
    pub async fn execute(self, child: BoxDiffStream) {
        #[for_await]
        for batch in child {
            let batch = batch?;
            let data = Evaluator::new(&self.projs).eval_list(batch.data())?;
            yield StreamChunk::new(batch.ops().to_vec(), data);
        }
    }
}
