use super::*;
use crate::binder::{BoundDrop, Object};

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalDrop {
    pub object: Object,
}

impl LogicalPlaner {
    pub fn plan_drop(&self, stmt: BoundDrop) -> Result<LogicalPlan, LogicalPlanError> {
        Ok(LogicalPlan::Drop(LogicalDrop {
            object: stmt.object,
        }))
    }
}
