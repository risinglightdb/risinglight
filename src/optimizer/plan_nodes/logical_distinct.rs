// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;

/// The logical plan of filter operation.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalDistinct {
    exprs: Vec<BoundExpr>,
    child: PlanRef,
}

impl LogicalDistinct {
    pub fn new(exprs: Vec<BoundExpr>, child: PlanRef) -> Self {
        Self { exprs, child }
    }

    pub fn distinct_exprs(&self) -> &[BoundExpr] {
        &self.exprs
    }

    pub fn clone_with_rewrite_expr(
        &self,
        new_child: PlanRef,
        rewriter: &impl ExprRewriter,
    ) -> Self {
        let mut new_exprs = self.exprs.clone();
        new_exprs
            .iter_mut()
            .for_each(|expr| rewriter.rewrite_expr(expr));
        LogicalDistinct::new(new_exprs, new_child)
    }
}
impl PlanTreeNodeUnary for LogicalDistinct {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.exprs.clone(), child)
    }
}

impl_plan_tree_node_for_unary!(LogicalDistinct);

impl PlanNode for LogicalDistinct {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.child.schema()
    }

    fn estimated_cardinality(&self) -> usize {
        self.child().estimated_cardinality()
    }

    fn prune_col(&self, required_cols: BitSet) -> PlanRef {
        LogicalDistinct::new(self.exprs.clone(), self.child.prune_col(required_cols))
            .into_plan_ref()
    }
}

impl fmt::Display for LogicalDistinct {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalDistinct")
    }
}
