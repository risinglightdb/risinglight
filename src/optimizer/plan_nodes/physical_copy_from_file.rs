use std::fmt;
use std::path::PathBuf;

use super::*;
use crate::binder::FileFormat;
use crate::types::DataType;

/// The physical plan of `COPY FROM`.
#[derive(Debug, Clone)]
pub struct PhysicalCopyFromFile {
    logcial: LogicalCopyFromFile,
}

impl PhysicalCopyFromFile {
    pub fn new(logcial: LogicalCopyFromFile) -> Self {
        Self { logcial }
    }

    /// Get a reference to the physical copy from file's logcial.
    pub fn logcial(&self) -> &LogicalCopyFromFile {
        &self.logcial
    }
}
impl PlanTreeNodeLeaf for LogicalCreateTable {}
impl_plan_tree_node_for_leaf!(LogicalCreateTable);

impl PlanNode for PhysicalCopyFromFile {}
impl fmt::Display for PhysicalCopyFromFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalCopyFromFile: path: {:?}, format: {:?}",
            self.path, self.format,
        )
    }
}
