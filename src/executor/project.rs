use super::*;
use crate::array::{ArrayImpl, DataChunk};
use crate::binder::BoundExpr;

pub struct ProjectionExecutor {
    pub project_expressions: Vec<BoundExpr>,
    pub child: mpsc::Receiver<DataChunk>,
    pub output: mpsc::Sender<DataChunk>,
}

impl ProjectionExecutor {
    pub async fn execute(mut self) -> Result<(), ExecutorError> {
        while let Some(batch) = self.child.recv().await {
            let arrays = self
                .project_expressions
                .iter()
                .map(|expr| expr.eval_array(&batch))
                .collect::<Vec<ArrayImpl>>();

            let result = DataChunk::builder()
                .cardinality(batch.cardinality())
                .arrays(arrays.into())
                .build();
            self.output.send(result).await.ok().unwrap();
        }
        Ok(())
    }
}
