use super::*;
use crate::{binder::BoundDrop, logical_optimizer::plan_nodes::logical_drop::LogicalDrop};

impl LogicalPlaner {
    pub fn plan_drop(&self, stmt: BoundDrop) -> Result<LogicalPlan, LogicalPlanError> {
        Ok(LogicalPlan::LogicalDrop(LogicalDrop {
            object: stmt.object,
        }))
    }
}
