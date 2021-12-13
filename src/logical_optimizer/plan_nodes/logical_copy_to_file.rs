use std::fmt;
use std::path::PathBuf;

use crate::binder::statement::copy::FileFormat;
use crate::logical_optimizer::plan_nodes::{Plan, PlanRef, UnaryLogicalPlanNode};
use crate::types::DataType;

/// The logical plan of `COPY TO`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalCopyToFile {
    /// The file path to copy to.
    pub path: PathBuf,
    /// The file format.
    pub format: FileFormat,
    /// The column types.
    pub column_types: Vec<DataType>,
    /// The child plan.
    pub child: PlanRef,
}

impl UnaryLogicalPlanNode for LogicalCopyToFile {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::LogicalCopyToFile(LogicalCopyToFile {
            path: self.path.clone(),
            format: self.format.clone(),
            column_types: self.column_types.clone(),
            child,
        })
        .into()
    }
}

impl fmt::Display for LogicalCopyToFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalCopyToFile: path: {:?}, format: {:?}",
            self.path, self.format,
        )
    }
}
