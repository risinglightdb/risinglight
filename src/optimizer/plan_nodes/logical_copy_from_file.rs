// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::path::PathBuf;

use serde::Serialize;

use super::*;
use crate::binder::statement::copy::FileFormat;
use crate::types::DataType;

/// The logical plan of `COPY FROM`.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalCopyFromFile {
    /// The file path to copy from.
    path: PathBuf,
    /// The file format.
    format: FileFormat,
    /// The column types.
    column_types: Vec<DataType>,
}
impl LogicalCopyFromFile {
    pub fn new(path: PathBuf, format: FileFormat, column_types: Vec<DataType>) -> Self {
        Self {
            path,
            format,
            column_types,
        }
    }

    /// Get a reference to the logical copy from file's format.
    pub fn format(&self) -> &FileFormat {
        &self.format
    }

    /// Get a reference to the logical copy from file's column types.
    pub fn column_types(&self) -> &[DataType] {
        self.column_types.as_ref()
    }

    /// Get a reference to the logical copy from file's path.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}
impl PlanTreeNodeLeaf for LogicalCopyFromFile {}
impl_plan_tree_node_for_leaf!(LogicalCopyFromFile);
impl PlanNode for LogicalCopyFromFile {
    fn out_types(&self) -> Vec<DataType> {
        self.column_types.clone()
    }
}

impl fmt::Display for LogicalCopyFromFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalCopyFromFile: path: {:?}, format: {:?}",
            self.path, self.format,
        )
    }
}
