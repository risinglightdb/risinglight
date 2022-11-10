// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;

/// The physical plan of `COPY TO`.
#[derive(Debug, Clone, Serialize)]
pub struct PhysicalCopyToFile {
    logical: LogicalCopyToFile,
}

impl PhysicalCopyToFile {
    pub fn new(logical: LogicalCopyToFile) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical copy to file's logical.
    pub fn logical(&self) -> &LogicalCopyToFile {
        &self.logical
    }
}

impl PlanTreeNodeUnary for PhysicalCopyToFile {
    fn child(&self) -> PlanRef {
        self.logical().child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logical().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalCopyToFile);
impl PlanNode for PhysicalCopyToFile {}

impl fmt::Display for PhysicalCopyToFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalCopyToFile: path: {:?}, format: {:?}",
            self.logical().path(),
            self.logical().format(),
        )
    }
}
