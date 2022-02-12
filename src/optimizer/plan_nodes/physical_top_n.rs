// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;

/// The physical plan of top N operation.
#[derive(Debug, Clone, Serialize)]
pub struct PhysicalTopN {
    logical: LogicalTopN,
}

impl PhysicalTopN {
    pub fn new(logical: LogicalTopN) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical top N's logical.
    pub fn logical(&self) -> &LogicalTopN {
        &self.logical
    }
}

impl PlanTreeNodeUnary for PhysicalTopN {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logical().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalTopN);
impl PlanNode for PhysicalTopN {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.logical().schema()
    }

    fn estimated_cardinality(&self) -> usize {
        self.logical().limit()
    }
}

impl fmt::Display for PhysicalTopN {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalTopN: offset: {}, limit: {}, order by {:?}",
            self.logical().offset(),
            self.logical().limit(),
            self.logical().comparators()
        )
    }
}
