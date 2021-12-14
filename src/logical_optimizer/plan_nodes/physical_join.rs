use std::fmt;

use crate::binder::BoundJoinOperator;
use crate::logical_optimizer::plan_nodes::logical_join::LogicalJoin;

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

/// Currently, we only use default join ordering.
/// We will implement DP or DFS algorithms for join orders.
impl PhysicalPlaner {
    pub fn plan_join(&self, logical_join: LogicalJoin) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Join(PhysicalJoin {
            join_type: PhysicalJoinType::NestedLoop,
            left_plan: self
                .plan_inner(logical_join.left_plan.as_ref().clone())?
                .into(),
            right_plan: self
                .plan_inner(logical_join.right_plan.as_ref().clone())?
                .into(),
            join_op: logical_join.join_op.clone(),
        }))
    }
}

impl fmt::Display for PhysicalJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalJoin: type {:?}, op {:?}",
            self.join_type, self.join_op
        )?;
    }
}
