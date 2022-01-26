// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;
use serde::{Serialize};
use itertools::Itertools;

use super::*;
use crate::catalog::TableRefId;
use crate::types::ColumnId;

/// The logical plan of `INSERT`.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalInsert {
    table_ref_id: TableRefId,
    column_ids: Vec<ColumnId>,
    child: PlanRef,
}

impl LogicalInsert {
    pub fn new(table_ref_id: TableRefId, column_ids: Vec<ColumnId>, child: PlanRef) -> Self {
        Self {
            table_ref_id,
            column_ids,
            child,
        }
    }

    /// Get a reference to the logical insert's table ref id.
    pub fn table_ref_id(&self) -> TableRefId {
        self.table_ref_id
    }

    /// Get a reference to the logical insert's column ids.
    pub fn column_ids(&self) -> &[u32] {
        self.column_ids.as_ref()
    }
}
impl PlanTreeNodeUnary for LogicalInsert {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.table_ref_id(), self.column_ids().to_vec(), child)
    }
}
impl_plan_tree_node_for_unary!(LogicalInsert);

impl PlanNode for LogicalInsert {}

impl fmt::Display for LogicalInsert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalInsert: table {}, columns [{}]",
            self.table_ref_id.table_id,
            self.column_ids.iter().map(ToString::to_string).join(", ")
        )
    }
}
