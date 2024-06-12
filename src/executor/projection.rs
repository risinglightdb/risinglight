// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::DataChunk;

/// The executor of project operation.
pub struct ProjectionExecutor {
    /// A list of expressions.
    ///
    /// e.g. `(list (+ #0 #1) #0)`
    pub projs: RecExpr,
}

impl ProjectionExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        #[for_await]
        for batch in child {
            println!("[project]\n{}", batch.clone().unwrap());
            println!("projs: {:#?}", self.projs);
            yield Evaluator::new(&self.projs).eval_list(&batch?)?;
        }
    }
}
