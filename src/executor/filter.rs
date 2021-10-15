use super::*;
use crate::array::{Array, ArrayImpl, DataChunk};
use crate::binder::BoundExpr;

/// The executor of a filter operation.
pub struct FilterExecutor {
    pub expr: BoundExpr,
    pub child: BoxedExecutor,
}

impl FilterExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            for await batch in self.child {
                let batch = batch?;
                let vis = match self.expr.eval_array(&batch)? {
                    ArrayImpl::Bool(a) => a,
                    _ => panic!("filters can only accept bool array"),
                };
                yield batch.filter(vis.iter().map(|b| matches!(b, Some(true))));
            }
        }
    }
}
