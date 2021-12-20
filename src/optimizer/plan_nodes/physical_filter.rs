use std::fmt;

use super::*;
use crate::binder::BoundExpr;

/// The physical plan of filter operation.
#[derive(Debug, Clone)]
pub struct PhysicalFilter {
    pub expr: BoundExpr,
    pub child: PlanRef,
}

impl_plan_tree_node!(PhysicalFilter, [child]);
impl PlanNode for PhysicalFilter {
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        rewriter.rewrite_expr(&mut self.expr);
    }
}
impl fmt::Display for PhysicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalFilter: expr {:?}", self.expr)
    }
}
