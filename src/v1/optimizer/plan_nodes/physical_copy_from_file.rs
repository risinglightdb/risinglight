// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;

/// The physical plan of `COPY FROM`.
#[derive(Debug, Clone, Serialize)]
pub struct PhysicalCopyFromFile {
    logical: LogicalCopyFromFile,
}

impl PhysicalCopyFromFile {
    pub fn new(logical: LogicalCopyFromFile) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical copy from file's logical.
    pub fn logical(&self) -> &LogicalCopyFromFile {
        &self.logical
    }
}
impl PlanTreeNodeLeaf for PhysicalCopyFromFile {}
impl_plan_tree_node_for_leaf!(PhysicalCopyFromFile);

impl PlanNode for PhysicalCopyFromFile {}
impl fmt::Display for PhysicalCopyFromFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalCopyFromFile: path: {:?}, format: {:?}",
            self.logical().path(),
            self.logical().format(),
        )
    }
}
