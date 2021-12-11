use super::*;
use crate::array::DataChunk;
use crate::binder::BoundExpr;

/// The executor of project operation.
pub struct ProjectionExecutor {
    pub project_expressions: Vec<BoundExpr>,
    pub child: BoxedExecutor,
}

impl ProjectionExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            for await batch in self.child {
                let batch = batch?;
                let chunk = self
                    .project_expressions
                    .iter()
                    .map(|expr| expr.eval_array(&batch))
                    .collect::<Result<DataChunk, _>>()?;
                yield chunk;
            }
        }
    }
}
