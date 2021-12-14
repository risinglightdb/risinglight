use super::*;
use crate::array::ArrayImpl;
use crate::physical_planner::PhysicalPlan;

/// The executor of `EXPLAIN` statement.
pub struct ExplainExecutor {
    pub plan: Box<PhysicalPlan>,
}

impl Executor for ExplainExecutor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError> {
        let explain_result = format!("{}", *self.plan);
        let chunk = DataChunk::from_iter([ArrayImpl::Utf8(
            [Some(explain_result)].into_iter().collect(),
        )]);
        Ok(chunk)
    }
}
