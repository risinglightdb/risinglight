use super::*;
use crate::array::{ArrayImpl, DataChunk};
use crate::binder::BoundExpr;

pub struct ProjectionExecutor {
    pub project_expressions: Vec<BoundExpr>,
    pub child: BoxedExecutor,
}

impl ProjectionExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            for await batch in self.child {
                let batch = batch?;
                let arrays = self
                    .project_expressions
                    .iter()
                    .map(|expr| expr.eval_array(&batch))
                    .collect::<Result<Vec<ArrayImpl>, _>>()?;

                yield DataChunk::builder()
                    .cardinality(batch.cardinality())
                    .arrays(arrays.into())
                    .build();
            }
        }
    }
}
