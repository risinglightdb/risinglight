use std::fmt;

use super::*;
use crate::binder::BoundOrderBy;

/// The logical plan of order.
#[derive(Debug, Clone)]
pub struct LogicalOrder {
    comparators: Vec<BoundOrderBy>,
    child: PlanRef,
}

impl LogicalOrder {
    pub fn new(comparators: Vec<BoundOrderBy>, child: PlanRef) -> Self {
        Self { comparators, child }
    }

    /// Get a reference to the logical order's comparators.
    pub fn comparators(&self) -> &[BoundOrderBy] {
        self.comparators.as_ref()
    }
}
impl PlanTreeNodeUnary for LogicalOrder {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.comparators(), child)
    }
}
impl_plan_tree_node_for_unary!(LogicalOrder);
impl PlanNode for LogicalOrder {
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        for cmp in &mut self.comparators {
            rewriter.rewrite_expr(&mut cmp.expr);
        }
    }
    fn out_types(&self) -> Vec<DataType> {
        self.child.out_types()
    }
}

impl fmt::Display for LogicalOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalOrder: {:?}", self.comparators)
    }
}
