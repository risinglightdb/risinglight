use super::*;
use crate::array::DataChunk;
use crate::binder::BoundExpr;

/// The executor of project operation.
pub struct ProjectionExecutor {
    pub exprs: Vec<BoundExpr>,
    pub child: BoxedExecutor,
}

impl Executor for ProjectionExecutor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError> {
        let chunk = self.child.execute()?;
        let chunk = self
            .exprs
            .iter()
            .map(|expr| expr.eval_array(&chunk))
            .collect::<Result<DataChunk, _>>()?;
        Ok(chunk)
    }
}
