use super::*;
use crate::binder::BoundDrop;
use crate::logical_optimizer::plan_nodes::logical_drop::LogicalDrop;

impl LogicalPlaner {
    pub fn plan_drop(&self, stmt: BoundDrop) -> Result<Plan, LogicalPlanError> {
        Ok(Plan::LogicalDrop(LogicalDrop {
            object: stmt.object,
        }))
    }
}
