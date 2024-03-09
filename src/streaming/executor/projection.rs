// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;

/// Evaluates an arbitrary list of expressions on its input.
pub struct Projection {
    /// A list of expressions.
    pub expr: ExpressionList,
}

impl Projection {
    #[try_stream(boxed, ok = DeltaBatch, error = Error)]
    async fn execute(self, input: DeltaBatchStream) {
        #[for_await]
        for batch in input {
            let batch = batch?;
            let projected = self.expr.eval(batch.data())?;
            yield DeltaBatch::new(batch.ops().clone(), projected);
        }
    }
}
