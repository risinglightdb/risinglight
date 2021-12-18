use std::fmt;

use super::*;
use crate::binder::BoundExpr;

/// The logical plan of project operation.
#[derive(Debug, Clone)]
pub struct LogicalProjection {
    pub project_expressions: Vec<BoundExpr>,
    pub child: PlanRef,
}

impl_plan_node!(LogicalProjection, [child]
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        for expr in &mut self.project_expressions {
            rewriter.rewrite_expr(expr);
        }
    }
);

impl fmt::Display for LogicalProjection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalProjection: exprs {:?}", self.project_expressions)
    }
}
