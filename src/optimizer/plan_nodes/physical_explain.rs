// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;

/// The physical plan of `EXPLAIN`.
#[derive(Debug, Clone, Serialize)]
pub struct PhysicalExplain {
    logical: LogicalExplain,
}

impl PhysicalExplain {
    pub fn new(logical: LogicalExplain) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical explain's logical.
    pub fn logical(&self) -> &LogicalExplain {
        &self.logical
    }
}

impl PlanTreeNodeUnary for PhysicalExplain {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logical().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalExplain);
impl PlanNode for PhysicalExplain {}
impl fmt::Display for PhysicalExplain {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalExplain:")
    }
}
