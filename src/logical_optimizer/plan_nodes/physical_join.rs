use std::fmt;

use super::{impl_plan_tree_node_for_binary, BinaryPlanNode, Plan, PlanRef, PlanTreeNode};
use crate::binder::BoundJoinOperator;

// The type of join algorithm.
// Before we have query optimzer. We only use nested loop join
#[derive(Clone, PartialEq, Debug)]
pub enum PhysicalJoinType {
    NestedLoop,
}
// The phyiscal plan of join
#[derive(Clone, PartialEq, Debug)]
pub struct PhysicalJoin {
    pub join_type: PhysicalJoinType,
    pub left_plan: PlanRef,
    pub right_plan: PlanRef,
    pub join_op: BoundJoinOperator,
}

impl BinaryPlanNode for PhysicalJoin {
    fn left(&self) -> PlanRef {
        self.left_plan.clone()
    }

    fn right(&self) -> PlanRef {
        self.right_plan.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> PlanRef {
        Plan::PhysicalJoin(PhysicalJoin {
            left_plan: left,
            right_plan: right,
            join_op: self.join_op.clone(),
            join_type: self.join_type.clone(),
        })
        .into()
    }
}
impl_plan_tree_node_for_binary! {PhysicalJoin}

/// Currently, we only use default join ordering.
/// We will implement DP or DFS algorithms for join orders.
impl fmt::Display for PhysicalJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalJoin: type {:?}, op {:?}",
            self.join_type, self.join_op
        )
    }
}
