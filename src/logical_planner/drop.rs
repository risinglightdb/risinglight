use super::*;
use crate::binder::BoundDrop;
use crate::optimizer::plan_nodes::LogicalDrop;

impl LogicalPlaner {
    pub fn plan_drop(&self, stmt: BoundDrop) -> Result<PlanRef, LogicalPlanError> {
        Ok(Rc::new(LogicalDrop::new(stmt.object)))
    }
}
