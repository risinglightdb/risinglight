use super::*;
use crate::optimizer::plan_nodes::LogicalExplain;

impl LogicalPlaner {
    pub fn plan_explain(&self, stmt: BoundStatement) -> Result<PlanRef, LogicalPlanError> {
        Ok(Rc::new(LogicalExplain {
            plan: self.plan(stmt)?,
        }))
    }
}
