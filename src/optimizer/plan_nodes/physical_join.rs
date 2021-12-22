use std::fmt;

use super::*;
use crate::binder::BoundJoinOperator;

/// The type of join algorithm.
///
/// Before we have query optimzer, we only use nested loop join.
#[derive(Clone, Debug)]
pub enum PhysicalJoinType {
    NestedLoop,
}

/// The phyiscal plan of join.
#[derive(Clone, Debug)]
pub struct PhysicalJoin {
    pub join_type: PhysicalJoinType,
    pub left_plan: PlanRef,
    pub right_plan: PlanRef,
    pub join_op: BoundJoinOperator,
    pub condition: BoundExpr,
    data_types: Vec<DataType>,
}

impl PhysicalJoin {
    pub fn new(
        join_type: PhysicalJoinType,
        left_plan: PlanRef,
        right_plan: PlanRef,
        join_op: BoundJoinOperator,
        condition: BoundExpr,
    ) -> Self {
        let mut data_types = left_plan.out_types();
        data_types.append(&mut right_plan.out_types());
        PhysicalJoin {
            join_type,
            left_plan,
            right_plan,
            join_op,
            condition,
            data_types,
        }
    }
}

impl_plan_tree_node!(PhysicalJoin, [left_plan, right_plan]);
impl PlanNode for PhysicalJoin {
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        rewriter.rewrite_expr(&mut self.condition);
    }
    fn out_types(&self) -> Vec<DataType> {
        self.data_types.clone()
    }
}
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
