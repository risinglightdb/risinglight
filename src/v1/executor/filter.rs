// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::{ArrayImpl, DataChunk};
use crate::v1::binder::BoundExpr;

/// The executor of a filter operation.
pub struct FilterExecutor {
    pub expr: BoundExpr,
    pub child: BoxedExecutor,
}

impl FilterExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        #[for_await]
        for batch in self.child {
            let batch = batch?;
            let vis = match self.expr.eval(&batch)? {
                ArrayImpl::Bool(a) => a,
                _ => panic!("filters can only accept bool array"),
            };
            yield batch.filter(vis.true_array());
        }
    }
}
