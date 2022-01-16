use std::fmt;

use super::*;
use crate::binder::BoundJoinOperator;

/// The logical plan of join, it only records join tables and operators.
///
/// The query optimizer should decide the join orders and specific algorithms (hash join, nested
/// loop join or index join).
#[derive(Debug, Clone)]
pub struct LogicalJoin {
    left_plan: PlanRef,
    right_plan: PlanRef,
    join_op: BoundJoinOperator,
    condition: BoundExpr,
    data_types: Vec<DataType>,
}

impl LogicalJoin {
    pub fn new(
        left_plan: PlanRef,
        right_plan: PlanRef,
        join_op: BoundJoinOperator,
        condition: BoundExpr,
    ) -> Self {
        let mut data_types = left_plan.out_types();
        data_types.append(&mut right_plan.out_types());
        LogicalJoin {
            left_plan,
            right_plan,
            join_op,
            data_types,
            condition,
        }
    }

    /// Get a reference to the logical join's join op.
    pub fn join_op(&self) -> BoundJoinOperator {
        self.join_op
    }

    /// Get a reference to the logical join's condition.
    pub fn condition(&self) -> &BoundExpr {
        &self.condition
    }

    /// Get a reference to the logical join's data types.
    pub fn data_types(&self) -> &[DataType] {
        self.data_types.as_ref()
    }
}
impl PlanTreeNodeBinary for LogicalJoin {
    fn left(&self) -> PlanRef {
        self.left_plan.clone()
    }
    fn right(&self) -> PlanRef {
        self.right_plan.clone()
    }

    #[must_use]
    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(left, right, self.join_op(), self.condition())
    }
}
impl PlanNode for LogicalJoin {
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        rewriter.rewrite_expr(&mut self.condition);
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
