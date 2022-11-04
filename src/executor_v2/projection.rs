// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::DataChunk;

/// The executor of project operation.
pub struct ProjectionExecutor {
    pub projs: Vec<RecExpr>,
}

impl ProjectionExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        #[for_await]
        for batch in child {
            let batch = batch?;
            let chunk: Vec<_> = self
                .projs
                .iter()
                .map(|expr| ExprRef::new(expr).eval(&batch))
                .try_collect()?;
            yield chunk.into_iter().collect();
        }
    }
}
