use std::fmt;

use super::*;
use crate::binder::{BoundJoinConstraint, BoundJoinOperator};

/// The logical plan of join, it only records join tables and operators.
///
/// The query optimizer should decide the join orders and specific algorithms (hash join, nested
/// loop join or index join).
#[derive(Debug, Clone)]
pub struct LogicalJoin {
    pub left_plan: PlanRef,
    pub right_plan: PlanRef,
    pub join_op: BoundJoinOperator,
    data_types: Vec<DataType>,
}

impl LogicalJoin {
    pub fn new(left_plan: PlanRef, right_plan: PlanRef, join_op: BoundJoinOperator) -> Self {
        let mut data_types = left_plan.out_types();
        data_types.append(&mut right_plan.out_types());
        LogicalJoin {
            left_plan,
            right_plan,
            join_op,
            data_types,
        }
    }
}

impl_plan_tree_node!(LogicalJoin, [left_plan, right_plan]);
impl PlanNode for LogicalJoin {
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
    fn out_types(&self) -> Vec<DataType> {
        self.data_types.clone()
    }
}

impl fmt::Display for LogicalJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalJoin: op {:?}", self.join_op)
    }
}
