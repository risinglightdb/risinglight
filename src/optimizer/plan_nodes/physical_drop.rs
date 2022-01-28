// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;

/// The physical plan of `DROP`.
#[derive(Debug, Clone, Serialize)]
pub struct PhysicalDrop {
    logical: LogicalDrop,
}

impl PhysicalDrop {
    pub fn new(logical: LogicalDrop) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical drop's logical.
    pub fn logical(&self) -> &LogicalDrop {
        &self.logical
    }
}

impl PlanTreeNodeLeaf for PhysicalDrop {}
impl_plan_tree_node_for_leaf!(PhysicalDrop);
impl PlanNode for PhysicalDrop {}

impl fmt::Display for PhysicalDrop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
