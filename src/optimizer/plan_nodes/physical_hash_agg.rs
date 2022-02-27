// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;

/// The physical plan of hash aggregation.
#[derive(Debug, Clone, Serialize)]
pub struct PhysicalHashAgg {
    logical: LogicalAggregate,
}

impl PhysicalHashAgg {
    pub fn new(logical: LogicalAggregate) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical hash agg's logical.
    pub fn logical(&self) -> &LogicalAggregate {
        &self.logical
    }
}
impl PlanTreeNodeUnary for PhysicalHashAgg {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logical().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalHashAgg);
impl PlanNode for PhysicalHashAgg {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.logical.schema()
    }

    fn estimated_cardinality(&self) -> usize {
        self.child().estimated_cardinality()
    }
}
impl fmt::Display for PhysicalHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalHashAgg:")?;
        for group_key in self.logical().group_keys().iter() {
            writeln!(f, "  {}", group_key)?
        }
        for agg in self.logical().agg_calls().iter() {
            writeln!(f, "  {}", agg)?
        }
        Ok(())
    }
}
