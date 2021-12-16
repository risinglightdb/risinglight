use super::*;
use crate::logical_optimizer::plan_nodes::logical_explain::LogicalExplain;

impl LogicalPlaner {
    pub fn plan_explain(&self, stmt: BoundStatement) -> Result<Plan, LogicalPlanError> {
        Ok(Plan::LogicalExplain(LogicalExplain {
            plan: (self.plan(stmt)?.into()),
        }))
    }
}
