use super::*;
use crate::{binder::BoundJoinOperator, logical_optimizer::plan_node::LogicalPlanNode};
/// The logical plan of join, it only records join tables and operators.
/// The query optimizer should decide the join orders and specific algorithms (hash join, nested
/// loop join or index join).
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalJoin {
    pub left_plan: LogicalPlanRef,
    pub right_plan: LogicalPlanRef,
    pub join_op: BoundJoinOperator,
}

impl LogicalPlanNode for LogicalJoin {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![self.left_plan.clone(), self.right_plan.clone()]
    }

    fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        LogicalPlan::LogicalJoin(LogicalJoin {
            left_plan: children[0].clone(),
            right_plan: children[1].clone(),
            join_op: self.join_op.clone(),
        })
        .into()
    }
}
