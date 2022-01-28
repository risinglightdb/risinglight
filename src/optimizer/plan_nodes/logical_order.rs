// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;
use crate::binder::BoundOrderBy;
use crate::optimizer::logical_plan_rewriter::ExprRewriter;

/// The logical plan of order.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalOrder {
    comparators: Vec<BoundOrderBy>,
    child: PlanRef,
}

impl LogicalOrder {
    pub fn new(comparators: Vec<BoundOrderBy>, child: PlanRef) -> Self {
        Self { comparators, child }
    }

    /// Get a reference to the logical order's comparators.
    pub fn comparators(&self) -> &[BoundOrderBy] {
        self.comparators.as_ref()
    }
    pub fn clone_with_rewrite_expr(
        &self,
        new_child: PlanRef,
        rewriter: &impl ExprRewriter,
    ) -> Self {
        let mut new_cmps = self.comparators().to_vec();
        for cmp in &mut new_cmps {
            rewriter.rewrite_expr(&mut cmp.expr);
        }
        LogicalOrder::new(new_cmps, new_child)
    }
}
impl PlanTreeNodeUnary for LogicalOrder {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.comparators().to_vec(), child)
    }
}
impl_plan_tree_node_for_unary!(LogicalOrder);
impl PlanNode for LogicalOrder {
    fn out_types(&self) -> Vec<DataType> {
        self.child.out_types()
    }
}

impl fmt::Display for LogicalOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalOrder: {:?}", self.comparators)
    }
}
