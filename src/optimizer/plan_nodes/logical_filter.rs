use std::fmt;

use super::*;
use crate::binder::BoundExpr;

/// The logical plan of filter operation.
#[derive(Debug, Clone)]
pub struct LogicalFilter {
    pub expr: BoundExpr,
    pub child: PlanRef,
}

impl_plan_tree_node!(LogicalFilter, [child]);
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
