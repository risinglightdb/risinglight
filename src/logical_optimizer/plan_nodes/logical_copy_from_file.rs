use std::path::PathBuf;

use crate::{binder::statement::copy::FileFormat, types::DataType};

/// The logical plan of `copy`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalCopyFromFile {
    /// The file path to copy from.
    pub path: PathBuf,
    /// The file format.
    pub format: FileFormat,
    /// The column types.
    pub column_types: Vec<DataType>,
}
