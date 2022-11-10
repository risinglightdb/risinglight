// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use indoc::indoc;
use serde::Serialize;

use super::*;

/// The phyiscal plan of join.
#[derive(Clone, Debug, Serialize)]
pub struct PhysicalNestedLoopJoin {
    logical: LogicalJoin,
}

impl PhysicalNestedLoopJoin {
    pub fn new(logical: LogicalJoin) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical nested loop join's logical.
    pub fn logical(&self) -> &LogicalJoin {
        &self.logical
    }
}

impl PlanTreeNodeBinary for PhysicalNestedLoopJoin {
    fn left(&self) -> PlanRef {
        self.logical.left()
    }
    fn right(&self) -> PlanRef {
        self.logical.right()
    }

    #[must_use]
    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(self.logical.clone_with_left_right(left, right))
    }
}
impl_plan_tree_node_for_binary!(PhysicalNestedLoopJoin);
impl PlanNode for PhysicalNestedLoopJoin {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.logical.schema()
    }

    fn estimated_cardinality(&self) -> usize {
        self.left().estimated_cardinality() * self.right().estimated_cardinality()
    }
}
/// Currently, we only use default join ordering.
/// We will implement DP or DFS algorithms for join orders.
impl fmt::Display for PhysicalNestedLoopJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            indoc! {"
                PhysicalNestedLoopJoin:
                  op {:?},
                  predicate: {}"},
            self.logical().join_op(),
            self.logical().predicate()
        )
    }
}
