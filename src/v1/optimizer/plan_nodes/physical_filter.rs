// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;

/// The physical plan of filter operation.
#[derive(Debug, Clone, Serialize)]
pub struct PhysicalFilter {
    logical: LogicalFilter,
}

impl PhysicalFilter {
    pub fn new(logical: LogicalFilter) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical filter's logical.
    pub fn logical(&self) -> &LogicalFilter {
        &self.logical
    }
}
impl PlanTreeNodeUnary for PhysicalFilter {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logical().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalFilter);
impl PlanNode for PhysicalFilter {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.logical.schema()
    }

    fn estimated_cardinality(&self) -> usize {
        self.child().estimated_cardinality()
    }
}
impl fmt::Display for PhysicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalFilter: expr {:?}", self.logical().expr())
    }
}
