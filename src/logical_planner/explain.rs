use crate::logical_optimizer::plan_node::UnaryLogicalPlanNode;

use super::*;

/// The logical plan of `explain`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalExplain {
    pub plan: LogicalPlanRef,
}

impl UnaryLogicalPlanNode for LogicalExplain {
    fn get_child(&self) -> LogicalPlanRef {
        self.plan.clone()
    }

    fn copy_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef {
        LogicalPlan::LogicalExplain(LogicalExplain { plan: child }).into()
    }
}

impl LogicalPlaner {
    pub fn plan_explain(&self, stmt: BoundStatement) -> Result<LogicalPlan, LogicalPlanError> {
        Ok(LogicalPlan::LogicalExplain(LogicalExplain {
            plan: (self.plan(stmt)?.into()),
        }))
    }
}
