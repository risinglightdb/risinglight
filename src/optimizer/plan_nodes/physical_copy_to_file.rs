use std::fmt;
use std::path::PathBuf;

use super::*;
use crate::binder::FileFormat;
use crate::types::DataType;

/// The physical plan of `COPY TO`.
#[derive(Debug, Clone)]
pub struct PhysicalCopyToFile {
    logcial: LogicalCopyToFile,
}

impl PlanTreeNodeUnary for PhysicalFilter {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logcial().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalFilter);
impl PlanNode for PhysicalCopyToFile {}

impl fmt::Display for PhysicalCopyToFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalCopyToFile: path: {:?}, format: {:?}",
            self.path, self.format,
        )
    }
}
