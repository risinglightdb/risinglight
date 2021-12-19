use super::*;
use crate::binder::BoundDrop;
use crate::logical_optimizer::plan_nodes::LogicalDrop;

impl LogicalPlaner {
    pub fn plan_drop(&self, stmt: BoundDrop) -> Result<PlanRef, LogicalPlanError> {
        Ok(Rc::new(LogicalDrop {
            object: stmt.object,
        }))
    }
}
