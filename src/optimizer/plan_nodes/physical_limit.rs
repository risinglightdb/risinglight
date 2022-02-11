// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;

/// The physical plan of limit operation.
#[derive(Debug, Clone, Serialize)]
pub struct PhysicalLimit {
    logical: LogicalLimit,
}

impl PhysicalLimit {
    pub fn new(logical: LogicalLimit) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical limit's logical.
    pub fn logical(&self) -> &LogicalLimit {
        &self.logical
    }
}

impl PlanTreeNodeUnary for PhysicalLimit {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logical().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalLimit);
impl PlanNode for PhysicalLimit {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.logical().schema()
    }

    fn estimated_cardinality(&self) -> usize {
        self.logical().limit()
    }
}

impl fmt::Display for PhysicalLimit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalLimit: offset: {}, limit: {}",
            self.logical().offset(),
            self.logical().limit()
        )
    }
}
