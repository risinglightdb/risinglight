use super::*;

/// The logical plan of `explain`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalExplain {
    pub plan: LogicalPlanRef,
}

impl LogicalPlaner {
    pub fn plan_explain(&self, stmt: BoundStatement) -> Result<LogicalPlan, LogicalPlanError> {
        Ok(LogicalPlan::Explain(LogicalExplain {
            plan: (self.plan(stmt)?.into()),
        }))
    }
}
