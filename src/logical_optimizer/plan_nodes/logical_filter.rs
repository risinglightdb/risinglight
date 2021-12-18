use std::fmt;

use super::*;
use crate::binder::BoundExpr;

/// The logical plan of filter operation.
#[derive(Debug, Clone)]
pub struct LogicalFilter {
    pub expr: BoundExpr,
    pub child: PlanRef,
}

impl_plan_node!(LogicalFilter, [child]
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        rewriter.rewrite_expr(&mut self.expr);
    }
);

impl fmt::Display for LogicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalFilter: expr {:?}", self.expr)
    }
}
