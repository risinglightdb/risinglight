use super::{LogicalPlan, LogicalPlanRef};
use crate::{binder::BoundJoinOperator, logical_optimizer::plan_nodes::BinaryLogicalPlanNode};

/// The logical plan of join, it only records join tables and operators.
/// The query optimizer should decide the join orders and specific algorithms (hash join, nested
/// loop join or index join).
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalJoin {
    pub left_plan: LogicalPlanRef,
    pub right_plan: LogicalPlanRef,
    pub join_op: BoundJoinOperator,
}

impl BinaryLogicalPlanNode for LogicalJoin {
    fn left(&self) -> LogicalPlanRef {
        self.left_plan.clone()
    }

    fn right(&self) -> LogicalPlanRef {
        self.right_plan.clone()
    }

    fn clone_with_left_right(&self, left: LogicalPlanRef, right: LogicalPlanRef) -> LogicalPlanRef {
        LogicalPlan::LogicalJoin(LogicalJoin {
            left_plan: left,
            right_plan: right,
            join_op: self.join_op.clone(),
        })
        .into()
    }
}
