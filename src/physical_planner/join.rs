use super::*;
use crate::binder::BoundJoinOperator;
use crate::logical_planner::LogicalJoin;
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
    pub left_plan: Box<PhysicalPlan>,
    pub right_plan: Box<PhysicalPlan>,
    pub join_op: BoundJoinOperator,
}

/// Currently, we only use default join ordering.
/// We will implement DP or DFS algorithms for join orders.
impl PhysicalPlaner {
    pub fn plan_join(&self, logical_join: LogicalJoin) -> Result<PhysicalPlan, PhysicalPlanError> {
        let mut plan = self.plan_inner(logical_join.relation_plan.as_ref().clone())?;
        for join_table in logical_join.join_table_plans.into_iter() {
            let join_table_plan = self.plan_inner(join_table.table_plan.as_ref().clone())?;
            plan = PhysicalPlan::Join(PhysicalJoin {
                join_type: PhysicalJoinType::NestedLoop,
                left_plan: plan.into(),
                right_plan: (join_table_plan.into()),
                join_op: join_table.join_op.clone(),
            })
        }

        Ok(plan)
    }
}

impl PlanExplainable for PhysicalJoin {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Join: type {:?}, op {:?}", self.join_type, self.join_op)?;
        self.left_plan.explain(level + 1, f)?;
        self.right_plan.explain(level + 1, f)
    }
}
