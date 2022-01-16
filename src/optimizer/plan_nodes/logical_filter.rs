use std::fmt;

use super::*;
use crate::binder::BoundExpr;

/// The logical plan of filter operation.
#[derive(Debug, Clone)]
pub struct LogicalFilter {
    expr: BoundExpr,
    child: PlanRef,
}

impl LogicalFilter {
    pub fn new(expr: BoundExpr, child: PlanRef) -> Self {
        Self { expr, child }
    }

    /// Get a reference to the logical filter's expr.
    pub fn expr(&self) -> &BoundExpr {
        &self.expr
    }
}
impl PlanTreeNodeUnary for LogicalFilter {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.expr(), child)
    }
}
impl_plan_tree_node_for_unary!(LogicalFilter);
impl PlanNode for LogicalFilter {
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        rewriter.rewrite_expr(&mut self.expr);
    }
    fn out_types(&self) -> Vec<DataType> {
        self.child.out_types()
    }
}

impl fmt::Display for LogicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalFilter: expr {:?}", self.expr)
    }
}
