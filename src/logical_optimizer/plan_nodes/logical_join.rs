use std::fmt;

use super::{impl_plan_tree_node_for_binary, Plan, PlanRef, PlanTreeNode};
use crate::binder::BoundJoinOperator;
use crate::logical_optimizer::plan_nodes::BinaryLogicalPlanNode;

/// The logical plan of join, it only records join tables and operators.
/// The query optimizer should decide the join orders and specific algorithms (hash join, nested
/// loop join or index join).
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalJoin {
    pub left_plan: PlanRef,
    pub right_plan: PlanRef,
    pub join_op: BoundJoinOperator,
}

impl BinaryLogicalPlanNode for LogicalJoin {
    fn left(&self) -> PlanRef {
        self.left_plan.clone()
    }

    fn right(&self) -> PlanRef {
        self.right_plan.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> PlanRef {
        Plan::LogicalJoin(LogicalJoin {
            left_plan: left,
            right_plan: right,
            join_op: self.join_op.clone(),
        })
        .into()
    }
}
impl_plan_tree_node_for_binary! {LogicalJoin}

impl fmt::Display for LogicalJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalJoin: op {:?}", self.join_op)
    }
}
