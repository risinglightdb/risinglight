use std::fmt;
use std::path::PathBuf;

use super::{
    impl_plan_tree_node_for_leaf, impl_plan_tree_node_for_unary, Plan, PlanRef, PlanTreeNode,
    UnaryLogicalPlanNode,
};
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
impl_plan_tree_node_for_leaf! {PhysicalCopyFromFile}

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

impl UnaryLogicalPlanNode for PhysicalCopyToFile {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::PhysicalCopyToFile(PhysicalCopyToFile {
            path: self.path.clone(),
            format: self.format.clone(),
            column_types: self.column_types.clone(),
            child,
        })
        .into()
    }
}
impl_plan_tree_node_for_unary! {PhysicalCopyToFile}

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
