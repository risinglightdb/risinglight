// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::DataChunk;
use crate::binder::BoundExpr;

/// The executor of project operation.
pub struct ProjectionExecutor {
    pub project_expressions: Vec<BoundExpr>,
    pub child: BoxedExecutor,
}

impl ProjectionExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        #[for_await]
        for batch in self.child {
            let batch = batch?;
            let chunk: Vec<_> = self
                .project_expressions
                .iter()
                .map(|expr| expr.eval(&batch))
                .try_collect()?;
            yield chunk.into_iter().collect();
        }
    }
}
