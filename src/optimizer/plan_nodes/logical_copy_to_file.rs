use std::fmt;
use std::path::PathBuf;

use super::*;
use crate::binder::statement::copy::FileFormat;
use crate::types::DataType;

/// The logical plan of `COPY TO`.
#[derive(Debug, Clone)]
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

impl_plan_tree_node!(LogicalCopyToFile, [child]);
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
