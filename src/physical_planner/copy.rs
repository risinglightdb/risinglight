use super::*;
use crate::{
    binder::FileFormat,
    logical_planner::{LogicalCopyFromFile, LogicalCopyToFile},
    types::DataType,
};
use std::path::PathBuf;

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
    pub child: Box<PhysicalPlan>,
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
            child: Box::new(self.plan_inner(*plan.child)?),
        }))
    }
}

impl PlanExplainable for PhysicalCopyFromFile {
    fn explain_inner(&self, _level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "CopyFromFile: path: {:?}, format: {:?}",
            self.path, self.format,
        )
    }
}

impl PlanExplainable for PhysicalCopyToFile {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "CopyToFile: path: {:?}, format: {:?}",
            self.path, self.format,
        )?;
        self.child.explain(level + 1, f)
    }
}
