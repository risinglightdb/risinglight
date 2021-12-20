use std::fmt;

use super::*;
use crate::binder::{BoundJoinConstraint, BoundJoinOperator};

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
}

impl_plan_tree_node!(PhysicalJoin, [left_plan, right_plan]);
impl PlanNode for PhysicalJoin {
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        use BoundJoinConstraint::*;
        use BoundJoinOperator::*;

        match &mut self.join_op {
            Inner(On(expr)) => rewriter.rewrite_expr(expr),
            LeftOuter(On(expr)) => rewriter.rewrite_expr(expr),
            RightOuter(On(expr)) => rewriter.rewrite_expr(expr),
            FullOuter(On(expr)) => rewriter.rewrite_expr(expr),
            CrossJoin => {}
        }
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
