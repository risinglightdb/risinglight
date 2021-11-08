use super::{FileFormat, PhysicalPlan};
use crate::types::DataType;
use std::path::PathBuf;

/// The physical plan of `copy`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalCopyToFile {
    /// The file path to copy to.
    pub path: PathBuf,
    /// The file format.
    pub format: FileFormat,
    /// The column types.
    pub column_types: Vec<DataType>,
    /// The child plan.
    pub child: PhysicalPlan,
}
