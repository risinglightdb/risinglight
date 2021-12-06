use crate::logical_optimizer::plan_nodes::logical_explain::LogicalExplain;

use super::*;

impl LogicalPlaner {
    pub fn plan_explain(&self, stmt: BoundStatement) -> Result<LogicalPlan, LogicalPlanError> {
        Ok(LogicalPlan::LogicalExplain(LogicalExplain {
            plan: (self.plan(stmt)?.into()),
        }))
    }
}
