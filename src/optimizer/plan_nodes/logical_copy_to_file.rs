use std::fmt;
use std::path::PathBuf;

use super::*;
use crate::binder::statement::copy::FileFormat;
use crate::types::DataType;

/// The logical plan of `COPY TO`.
#[derive(Debug, Clone)]
pub struct LogicalCopyToFile {
    /// The file path to copy to.
    path: PathBuf,
    /// The file format.
    format: FileFormat,
    /// The column types.
    column_types: Vec<DataType>,
    /// The child plan.
    child: PlanRef,
}
impl LogicalCopyToFile {
    fn new(path: PathBuf, format: FileFormat, column_types: Vec<DataType>, child: PlanRef) {
        Self {
            path,
            format,
            column_types,
            child,
        }
    }
    fn get_path(&self) -> &PathBuf {
        &self.path
    }
    fn get_file_format(&self) -> &PathBuf {
        &self.format
    }
    fn get_column_types(&self) -> &PathBuf {
        &self.column_types
    }
}
impl PlanTreeNodeUnary for LogicalCopyToFile {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(
            self.get_path(),
            self.get_file_format(),
            self.get_column_types(),
            child,
        )
    }
}
impl_plan_tree_node_for_unary!(LogicalCopyToFile);
impl PlanNode for LogicalCopyToFile {}

impl fmt::Display for LogicalCopyToFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalCopyToFile: path: {:?}, format: {:?}",
            self.path, self.format,
        )
    }
}
