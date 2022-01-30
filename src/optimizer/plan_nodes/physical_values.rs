// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;

/// The physical plan of `VALUES`.
#[derive(Debug, Clone, Serialize)]
pub struct PhysicalValues {
    logical: LogicalValues,
}

impl PhysicalValues {
    pub fn new(logical: LogicalValues) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical values's logical.
    pub fn logical(&self) -> &LogicalValues {
        &self.logical
    }
}

impl PlanTreeNodeLeaf for PhysicalValues {}
impl_plan_tree_node_for_leaf!(PhysicalValues);
impl PlanNode for PhysicalValues {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.logical().schema()
    }
}
impl fmt::Display for PhysicalValues {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalValues: {} rows", self.logical().values().len())
    }
}
