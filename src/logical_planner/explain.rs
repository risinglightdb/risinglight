use super::*;
use crate::optimizer::plan_nodes::LogicalExplain;

impl LogicalPlaner {
    pub fn plan_explain(&self, stmt: BoundStatement) -> Result<PlanRef, LogicalPlanError> {
        Ok(Arc::new(LogicalExplain::new(self.plan(stmt)?)))
    }
}
