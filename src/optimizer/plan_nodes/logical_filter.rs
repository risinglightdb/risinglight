// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;
use crate::binder::BoundExpr;
use crate::optimizer::logical_plan_rewriter::ExprRewriter;

/// The logical plan of filter operation.
#[derive(Debug, Clone, Serialize)]
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
    pub fn clone_with_rewrite_expr(
        &self,
        new_child: PlanRef,
        rewriter: &impl ExprRewriter,
    ) -> Self {
        let mut new_expr = self.expr().clone();
        rewriter.rewrite_expr(&mut new_expr);
        LogicalFilter::new(new_expr, new_child)
    }
}
impl PlanTreeNodeUnary for LogicalFilter {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.expr().clone(), child)
    }
}
impl_plan_tree_node_for_unary!(LogicalFilter);
impl PlanNode for LogicalFilter {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.child.schema()
    }

    fn estimated_cardinality(&self) -> usize {
        self.child().estimated_cardinality()
    }
}

impl fmt::Display for LogicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalFilter: expr {:?}", self.expr)
    }
}
