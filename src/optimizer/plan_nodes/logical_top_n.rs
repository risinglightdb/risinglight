// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;
use crate::binder::statement::BoundOrderBy;

/// The logical plan of top N operation.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalTopN {
    offset: usize,
    limit: usize,
    comparators: Vec<BoundOrderBy>,
    child: PlanRef,
}

impl LogicalTopN {
    pub fn new(
        offset: usize,
        limit: usize,
        comparators: Vec<BoundOrderBy>,
        child: PlanRef,
    ) -> Self {
        Self {
            offset,
            limit,
            comparators,
            child,
        }
    }

    /// Get a reference to the logical top N's offset.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Get a reference to the logical top N's limit.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Get a reference to the logical top N's comparators.
    pub fn comparators(&self) -> &[BoundOrderBy] {
        self.comparators.as_ref()
    }
}
impl PlanTreeNodeUnary for LogicalTopN {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(
            self.offset(),
            self.limit(),
            self.comparators().to_owned(),
            child,
        )
    }
}
impl_plan_tree_node_for_unary!(LogicalTopN);
impl PlanNode for LogicalTopN {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.child.schema()
    }

    fn estimated_cardinality(&self) -> usize {
        self.limit
    }
}

impl fmt::Display for LogicalTopN {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalTopN: offset: {}, limit: {}, order by {:?}",
            self.offset, self.limit, self.comparators
        )
    }
}
