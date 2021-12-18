use std::fmt;

use super::*;
use crate::binder::BoundJoinOperator;

// The type of join algorithm.
// Before we have query optimzer. We only use nested loop join
#[derive(Clone, Debug)]
pub enum PhysicalJoinType {
    NestedLoop,
}

// The phyiscal plan of join
#[derive(Clone, Debug)]
pub struct PhysicalJoin {
    pub join_type: PhysicalJoinType,
    pub left_plan: PlanRef,
    pub right_plan: PlanRef,
    pub join_op: BoundJoinOperator,
}

impl_plan_node!(PhysicalJoin, [left_plan, right_plan]);

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
