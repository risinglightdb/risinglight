use std::fmt;
use std::path::PathBuf;

use super::PlanRef;
use crate::binder::FileFormat;
use crate::logical_optimizer::plan_nodes::logical_copy_from_file::LogicalCopyFromFile;
use crate::logical_optimizer::plan_nodes::logical_copy_to_file::LogicalCopyToFile;
use crate::physical_planner::*;
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

impl PhysicalPlaner {
    pub fn plan_copy_from_file(
        &self,
        plan: LogicalCopyFromFile,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::CopyFromFile(PhysicalCopyFromFile {
            path: plan.path,
            format: plan.format,
            column_types: plan.column_types,
        }))
    }

    pub fn plan_copy_to_file(
        &self,
        plan: LogicalCopyToFile,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::CopyToFile(PhysicalCopyToFile {
            path: plan.path,
            format: plan.format,
            column_types: plan.column_types,
            child: self.plan_inner(plan.child.as_ref().clone())?.into(),
        }))
    }
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

impl fmt::Display for LogicalCopyToFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalCopyFromFile: path: {:?}, format: {:?}",
            self.path, self.format,
        )
    }
}
