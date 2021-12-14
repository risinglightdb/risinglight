use std::fmt;
use std::path::PathBuf;

use super::PlanRef;
use crate::binder::FileFormat;
use crate::types::DataType;

/// The physical plan of `COPY FROM`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalCopyFromFile {
    /// The file path to copy from.
    pub path: PathBuf,
    /// The file format.
    pub format: FileFormat,
    /// The column types.
    pub column_types: Vec<DataType>,
}

/// The physical plan of `COPY TO`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalCopyToFile {
    /// The file path to copy to.
    pub path: PathBuf,
    /// The file format.
    pub format: FileFormat,
    /// The column types.
    pub column_types: Vec<DataType>,
    /// The child plan.
    pub child: PlanRef,
}

impl fmt::Display for PhysicalCopyFromFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalCopyFromFile: path: {:?}, format: {:?}",
            self.path, self.format,
        )
    }
}

impl fmt::Display for PhysicalCopyToFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalCopyToFile: path: {:?}, format: {:?}",
            self.path, self.format,
        )
    }
}
