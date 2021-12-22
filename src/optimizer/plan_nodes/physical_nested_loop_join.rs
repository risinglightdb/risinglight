use std::fmt;

use super::*;
use crate::binder::BoundJoinOperator;
/// The phyiscal plan of join.
#[derive(Clone, Debug)]
pub struct PhysicalNestedLoopJoin {
    pub left_plan: PlanRef,
    pub right_plan: PlanRef,
    pub join_op: BoundJoinOperator,
    pub condition: BoundExpr,
    data_types: Vec<DataType>,
}

impl PhysicalNestedLoopJoin {
    pub fn new(
        left_plan: PlanRef,
        right_plan: PlanRef,
        join_op: BoundJoinOperator,
        condition: BoundExpr,
    ) -> Self {
        let mut data_types = left_plan.out_types();
        data_types.append(&mut right_plan.out_types());
        PhysicalNestedLoopJoin {
            left_plan,
            right_plan,
            join_op,
            condition,
            data_types,
        }
    }
}

impl_plan_tree_node!(PhysicalNestedLoopJoin, [left_plan, right_plan]);
impl PlanNode for PhysicalNestedLoopJoin {
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        rewriter.rewrite_expr(&mut self.condition);
    }
    fn out_types(&self) -> Vec<DataType> {
        self.data_types.clone()
    }
}
/// Currently, we only use default join ordering.
/// We will implement DP or DFS algorithms for join orders.
impl fmt::Display for PhysicalNestedLoopJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalNestedLoopJoin: op {:?}", self.join_op)
    }
}
