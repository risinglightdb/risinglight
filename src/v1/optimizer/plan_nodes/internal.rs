// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use itertools::Itertools;
use serde::Serialize;

use super::*;
use crate::catalog::{ColumnDesc, ColumnId, TableRefId};

/// The logical plan of scanning internal tables.
#[derive(Debug, Clone, Serialize)]
pub struct Internal {
    table_name: String,
    table_ref_id: TableRefId,
    column_ids: Vec<ColumnId>,
    column_descs: Vec<ColumnDesc>,
}

impl Internal {
    pub fn new(
        table_name: String,
        table_ref_id: TableRefId,
        column_ids: Vec<ColumnId>,
        column_descs: Vec<ColumnDesc>,
    ) -> Self {
        Self {
            table_name,
            table_ref_id,
            column_ids,
            column_descs,
        }
    }

    /// Get a reference to the logical table scan's table ref id.
    pub fn table_ref_id(&self) -> TableRefId {
        self.table_ref_id
    }

    /// Get a reference to the logical table scan's column ids.
    pub fn column_ids(&self) -> &[u32] {
        self.column_ids.as_ref()
    }

    /// Get a reference to the logical table scan's column descs.
    pub fn column_descs(&self) -> &[ColumnDesc] {
        self.column_descs.as_ref()
    }

    pub fn table_name(&self) -> String {
        self.table_name.clone()
    }
}

impl PlanTreeNodeLeaf for Internal {}
impl_plan_tree_node_for_leaf!(Internal);

impl PlanNode for Internal {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.column_descs.clone()
    }

    fn prune_col(&self, required_cols: BitSet) -> PlanRef {
        let (column_ids, column_descs) = required_cols
            .iter()
            .map(|col_idx| (self.column_ids[col_idx], self.column_descs[col_idx].clone()))
            .unzip();
        Internal::new(
            self.table_name.clone(),
            self.table_ref_id,
            column_ids,
            column_descs,
        )
        .into_plan_ref()
    }
}

impl fmt::Display for Internal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalInternal: table #{}, columns [{}]",
            self.table_ref_id.table_id,
            self.column_ids.iter().map(ToString::to_string).join(", "),
        )
    }
}
