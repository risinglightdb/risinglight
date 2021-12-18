use std::fmt;
use std::path::PathBuf;

use super::*;
use crate::binder::FileFormat;
use crate::types::DataType;

/// The physical plan of `COPY TO`.
#[derive(Debug, Clone)]
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

impl_plan_node!(PhysicalCopyToFile, [child]);

impl fmt::Display for PhysicalCopyToFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalCopyToFile: path: {:?}, format: {:?}",
            self.path, self.format,
        )
    }
}
