use std::fmt;

use super::*;
use crate::binder::{BoundJoinConstraint, BoundJoinOperator};

/// The logical plan of join, it only records join tables and operators.
/// The query optimizer should decide the join orders and specific algorithms (hash join, nested
/// loop join or index join).
#[derive(Debug, Clone)]
pub struct LogicalJoin {
    pub left_plan: PlanRef,
    pub right_plan: PlanRef,
    pub join_op: BoundJoinOperator,
}

impl_plan_node!(LogicalJoin, [left_plan, right_plan]
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        use BoundJoinOperator::*;
        use BoundJoinConstraint::*;

        match &mut self.join_op {
            Inner(On(expr)) => rewriter.rewrite_expr(expr),
            LeftOuter(On(expr)) => rewriter.rewrite_expr(expr),
            RightOuter(On(expr)) => rewriter.rewrite_expr(expr),
            FullOuter(On(expr)) => rewriter.rewrite_expr(expr),
            CrossJoin => {}
        }
    }
);

impl fmt::Display for LogicalJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalJoin: op {:?}", self.join_op)
    }
}
