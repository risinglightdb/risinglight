// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;

/// The logical plan of limit operation.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalLimit {
    offset: usize,
    limit: usize,
    child: PlanRef,
}

impl LogicalLimit {
    pub fn new(offset: usize, limit: usize, child: PlanRef) -> Self {
        Self {
            offset,
            limit,
            child,
        }
    }

    /// Get a reference to the logical limit's offset.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Get a reference to the logical limit's limit.
    pub fn limit(&self) -> usize {
        self.limit
    }
    #[allow(dead_code)]
    fn estimated_cardinality(&self) -> usize {
        self.limit
    }
}
impl PlanTreeNodeUnary for LogicalLimit {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.offset(), self.limit(), child)
    }
}
impl_plan_tree_node_for_unary!(LogicalLimit);
impl PlanNode for LogicalLimit {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.child.schema()
    }

    fn prune_col(&self, required_cols: BitSet) -> PlanRef {
        LogicalLimit::new(self.offset, self.limit, self.child.prune_col(required_cols))
            .into_plan_ref()
    }
}

impl fmt::Display for LogicalLimit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalLimit: offset: {}, limit: {}",
            self.offset, self.limit
        )
    }
}
