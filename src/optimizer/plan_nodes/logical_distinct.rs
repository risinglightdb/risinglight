// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;

/// The logical plan of filter operation.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalDistinct {
    child: PlanRef,
}

impl LogicalDistinct {
    pub fn new(child: PlanRef) -> Self {
        Self { child }
    }

    pub fn clone_with_rewrite_expr(
        &self,
        new_child: PlanRef,
        _rewriter: &impl ExprRewriter,
    ) -> Self {
        LogicalDistinct::new(new_child)
    }
}
impl PlanTreeNodeUnary for LogicalDistinct {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(child)
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
        LogicalDistinct::new(self.child.prune_col(required_cols)).into_plan_ref()
    }
}

impl fmt::Display for LogicalDistinct {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalDistinct")
    }
}
