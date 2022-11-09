// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::path::PathBuf;

use serde::Serialize;

use super::*;
use crate::types::DataType;
use crate::v1::binder::statement::copy::FileFormat;

/// The logical plan of `COPY TO`.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalCopyToFile {
    /// The file path to copy to.
    path: PathBuf,
    /// The file format.
    format: FileFormat,
    /// The column types.
    column_types: Vec<DataType>,
    /// The child plan.
    child: PlanRef,
}
impl LogicalCopyToFile {
    pub fn new(
        path: PathBuf,
        format: FileFormat,
        column_types: Vec<DataType>,
        child: PlanRef,
    ) -> Self {
        Self {
            path,
            format,
            column_types,
            child,
        }
    }

    /// Get a reference to the logical copy to file's path.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Get a reference to the logical copy to file's format.
    pub fn format(&self) -> &FileFormat {
        &self.format
    }

    /// Get a reference to the logical copy to file's column types.
    pub fn column_types(&self) -> &[DataType] {
        self.column_types.as_ref()
    }
}
impl PlanTreeNodeUnary for LogicalCopyToFile {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(
            self.path().clone(),
            self.format().clone(),
            self.column_types().to_vec(),
            child,
        )
    }
}
impl_plan_tree_node_for_unary!(LogicalCopyToFile);
impl PlanNode for LogicalCopyToFile {
    fn prune_col(&self, _required_cols: BitSet) -> PlanRef {
        let input_cols = (0..self.child().out_types().len()).into_iter().collect();
        self.clone_with_child(self.child.prune_col(input_cols))
            .into_plan_ref()
    }
}

impl fmt::Display for LogicalCopyToFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalCopyToFile: path: {:?}, format: {:?}",
            self.path, self.format,
        )
    }
}
