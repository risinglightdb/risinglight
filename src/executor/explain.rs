use super::*;
use crate::physical_planner::PhysicalExplain;

/// The executor of `explain` statement.
pub struct ExplainExecutor {
    pub plan: PhysicalExplain,
}

impl ExplainExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        println!("{:?}", self.plan.plan);
        try_stream! {
            yield DataChunk::single();
        }
    }
}
