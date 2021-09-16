use super::*;
use crate::array::{ArrayImpl, DataChunk};
use crate::parser::Expression;

pub struct ProjectionExecutor {
    pub project_expressions: Vec<Expression>,
    pub child_executor: BoxedExecutor,
}

impl ProjectionExecutor {
    pub async fn execute(self) -> Result<ExecutorResult, ExecutorError> {
        let child_res = self.child_executor.await?;
        match &child_res {
            ExecutorResult::Batch(batch) => {
                let arrays = self
                    .project_expressions
                    .iter()
                    .map(|expr| expr.eval_array(batch))
                    .collect::<Vec<ArrayImpl>>();

                let result = DataChunk::builder()
                    .cardinality(batch.cardinality())
                    .arrays(arrays.into())
                    .build();
                Ok(ExecutorResult::Batch(result))
            }
            ExecutorResult::Empty => Ok(ExecutorResult::Empty),
        }
    }
}
