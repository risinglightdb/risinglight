use std::fmt;
use std::path::PathBuf;

use super::*;
use crate::binder::statement::copy::FileFormat;
use crate::types::DataType;

/// The logical plan of `COPY FROM`.
#[derive(Debug, Clone)]
pub struct LogicalCopyFromFile {
    /// The file path to copy from.
    pub path: PathBuf,
    /// The file format.
    pub format: FileFormat,
    /// The column types.
    pub column_types: Vec<DataType>,
}

impl_plan_tree_node!(LogicalCopyFromFile);
impl PlanNode for LogicalCopyFromFile {}

impl fmt::Display for LogicalCopyFromFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalCopyFromFile: path: {:?}, format: {:?}",
            self.path, self.format,
        )
    }
}
