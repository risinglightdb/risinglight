use super::*;
use crate::logical_planner::LogicalJoin;
use crate::binder::BoundJoinOperator;
// The type of join algorithm.
// Before we have query optimzer. We only use nested loop join
#[derive(Clone, PartialEq, Debug)]
pub enum PhysicalJoinType {
    NestedLoop
}
// The phyiscal plan of join
#[derive(Clone, PartialEq, Debug)]
pub struct PhysicalJoin {
    pub join_type: PhysicalJoinType,
    pub left_plan: Box<PhysicalPlan>,
    pub right_plan: Box<PhysicalPlan>,
    pub join_op: BoundJoinOperator
}

impl PhysicalPlaner {
    pub fn plan_join(&self, logical_join: LogicalJoin) -> Result<PhysicalPlan, PhysicalPlanError> {
        let left_plan = self.plan(*logical_join.left_plan)?;
        let right_plan = self.plan(*logical_join.right_plan)?;
        Ok(PhysicalPlan::Join(PhysicalJoin{
            join_type: PhysicalJoinType::NestedLoop,
            left_plan: Box::new(left_plan),
            right_plan: Box::new(right_plan),
            join_op: logical_join.join_op.clone()
        }))
    }
}
