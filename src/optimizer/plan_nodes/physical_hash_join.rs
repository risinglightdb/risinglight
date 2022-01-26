// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;
use serde::{Serialize};
use super::*;

/// The phyiscal plan of join.
#[derive(Clone, Debug, Serialize)]
pub struct PhysicalHashJoin {
    logical: LogicalJoin,
    left_column_index: usize,
    right_column_index: usize,
}

impl PhysicalHashJoin {
    pub fn new(logical: LogicalJoin, left_column_index: usize, right_column_index: usize) -> Self {
        Self {
            logical,
            left_column_index,
            right_column_index,
        }
    }

    /// Get a reference to the physical hash join's logical.
    pub fn logical(&self) -> &LogicalJoin {
        &self.logical
    }

    /// Get a reference to the physical hash join's left column index.
    pub fn left_column_index(&self) -> usize {
        self.left_column_index
    }

    /// Get a reference to the physical hash join's right column index.
    pub fn right_column_index(&self) -> usize {
        self.right_column_index
    }
}
impl PlanTreeNodeBinary for PhysicalHashJoin {
    fn left(&self) -> PlanRef {
        self.logical.left()
    }
    fn right(&self) -> PlanRef {
        self.logical.right()
    }

    #[must_use]
    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(
            self.logical.clone_with_left_right(left, right),
            self.left_column_index(),
            self.right_column_index(),
        )
    }
}
impl_plan_tree_node_for_binary!(PhysicalHashJoin);

impl PlanNode for PhysicalHashJoin {
    fn out_types(&self) -> Vec<DataType> {
        self.logical().out_types()
    }
}
/// Currently, we only use default join ordering.
/// We will implement DP or DFS algorithms for join orders.
impl fmt::Display for PhysicalHashJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalHashJoin: op {:?}, predicate: {:?}",
            self.logical().join_op(),
            self.logical().predicate()
        )
    }
}
