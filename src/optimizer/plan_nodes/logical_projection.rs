use std::fmt;

use super::*;
use crate::binder::BoundExpr;
use crate::optimizer::logical_plan_rewriter::ExprRewriter;

/// The logical plan of project operation.
#[derive(Debug, Clone)]
pub struct LogicalProjection {
    project_expressions: Vec<BoundExpr>,
    child: PlanRef,
}

impl LogicalProjection {
    pub fn new(project_expressions: Vec<BoundExpr>, child: PlanRef) -> Self {
        Self {
            project_expressions,
            child,
        }
    }

    /// Get a reference to the logical projection's project expressions.
    pub fn project_expressions(&self) -> &[BoundExpr] {
        self.project_expressions.as_ref()
    }
    pub fn clone_with_rewrite_expr(&self, new_child: PlanRef, rewriter: impl ExprRewriter) -> Self {
        let new_exprs = self
            .project_expressions()
            .iter()
            .cloned()
            .foreach(|expr| self.rewrite_expr(&mut expr))
            .collect();
        LogicalProjection::new(new_exprs, new_child)
    }
}
impl PlanTreeNodeUnary for LogicalProjection {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.project_expressions(), child)
    }
}
impl_plan_tree_node_for_unary!(LogicalProjection);
impl PlanNode for LogicalProjection {
    fn out_types(&self) -> Vec<DataType> {
        self.project_expressions
            .iter()
            .map(|expr| expr.return_type().unwrap())
            .collect()
    }
}

impl fmt::Display for LogicalProjection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalProjection: exprs {:?}", self.project_expressions)
    }
}
