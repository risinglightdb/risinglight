use std::fmt;

use super::*;
use crate::binder::BoundExpr;

/// The physical plan of project operation.
#[derive(Debug, Clone)]
pub struct PhysicalProjection {
    pub project_expressions: Vec<BoundExpr>,
    pub child: PlanRef,
}

impl_plan_tree_node!(PhysicalProjection, [child]);
impl PlanNode for PhysicalProjection {
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        for expr in &mut self.project_expressions {
            rewriter.rewrite_expr(expr);
        }
    }
}

impl fmt::Display for PhysicalProjection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalProjection: exprs {:?}",
            self.project_expressions
        )
    }
}
