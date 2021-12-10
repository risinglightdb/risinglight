use crate::{
    binder::statement::copy::FileFormat,
    logical_optimizer::plan_nodes::{LogicalPlan, LogicalPlanRef, UnaryLogicalPlanNode},
    types::DataType,
};
use std::path::PathBuf;

/// The logical plan of `COPY TO`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalCopyToFile {
    /// The file path to copy to.
    pub path: PathBuf,
    /// The file format.
    pub format: FileFormat,
    /// The column types.
    pub column_types: Vec<DataType>,
    /// The child plan.
    pub child: LogicalPlanRef,
}

impl UnaryLogicalPlanNode for LogicalCopyToFile {
    fn child(&self) -> LogicalPlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef {
        LogicalPlan::LogicalCopyToFile(LogicalCopyToFile {
            path: self.path.clone(),
            format: self.format.clone(),
            column_types: self.column_types.clone(),
            child,
        })
        .into()
    }
}
