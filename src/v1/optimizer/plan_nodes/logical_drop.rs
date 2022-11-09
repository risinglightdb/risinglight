// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;
use crate::types::DataTypeKind;
use crate::v1::binder::statement::drop::Object;

/// The logical plan of `DROP`.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalDrop {
    object: Object,
}

impl LogicalDrop {
    pub fn new(object: Object) -> Self {
        Self { object }
    }

    /// Get a reference to the logical drop's object.
    pub fn object(&self) -> &Object {
        &self.object
    }
}
impl PlanTreeNodeLeaf for LogicalDrop {}
impl_plan_tree_node_for_leaf!(LogicalDrop);
impl PlanNode for LogicalDrop {
    fn schema(&self) -> Vec<ColumnDesc> {
        vec![ColumnDesc::new(
            DataType::new(DataTypeKind::Int32, false),
            "$drop".to_string(),
            false,
        )]
    }
}

impl fmt::Display for LogicalDrop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
