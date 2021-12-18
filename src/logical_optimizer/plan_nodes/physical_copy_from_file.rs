use std::fmt;
use std::path::PathBuf;

use super::*;
use crate::binder::FileFormat;
use crate::types::DataType;

/// The physical plan of `COPY FROM`.
#[derive(Debug, Clone)]
pub struct PhysicalCopyFromFile {
    /// The file path to copy from.
    pub path: PathBuf,
    /// The file format.
    pub format: FileFormat,
    /// The column types.
    pub column_types: Vec<DataType>,
}

impl_plan_node!(PhysicalCopyFromFile);

impl fmt::Display for PhysicalCopyFromFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalCopyFromFile: path: {:?}, format: {:?}",
            self.path, self.format,
        )
    }
}
