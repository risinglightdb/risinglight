use super::*;
use crate::array::{ArrayImpl, UTF8Array};
use crate::physical_planner::PhysicalExplain;

/// The executor of `explain` statement.
pub struct ExplainExecutor {
    pub plan: PhysicalExplain,
}

impl ExplainExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        let explain_result = format!("{}", self.plan.plan);
        let chunk = DataChunk::from_iter([ArrayImpl::UTF8(UTF8Array::from_iter([Some(
            explain_result,
        )]))]);

        try_stream! {
            yield chunk;
        }
    }
}
