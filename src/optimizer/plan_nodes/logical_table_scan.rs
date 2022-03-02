// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;


use itertools::Itertools;
use serde::Serialize;

use super::*;
use crate::catalog::{ColumnDesc, TableRefId};
use crate::types::ColumnId;
/// The logical plan of sequential scan operation.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalTableScan {
    table_ref_id: TableRefId,
    column_ids: Vec<ColumnId>,
    column_descs: Vec<ColumnDesc>,
    with_row_handler: bool,
    is_sorted: bool,
    expr: Option<BoundExpr>,
}

impl LogicalTableScan {
    pub fn new(
        table_ref_id: TableRefId,
        column_ids: Vec<ColumnId>,
        column_descs: Vec<ColumnDesc>,
        with_row_handler: bool,
        is_sorted: bool,
        expr: Option<BoundExpr>,
    ) -> Self {
        Self {
            table_ref_id,
            column_ids,
            column_descs,
            with_row_handler,
            is_sorted,
            expr,
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

    /// Get a reference to the logical table scan's with row handler.
    pub fn with_row_handler(&self) -> bool {
        self.with_row_handler
    }

    /// Get a reference to the logical table scan's is sorted.
    pub fn is_sorted(&self) -> bool {
        self.is_sorted
    }

    /// Get a reference to the logical table scan's expr.
    pub fn expr(&self) -> Option<&BoundExpr> {
        self.expr.as_ref()
    }
}
impl PlanTreeNodeLeaf for LogicalTableScan {}
impl_plan_tree_node_for_leaf!(LogicalTableScan);

impl PlanNode for LogicalTableScan {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.column_descs.clone()
    }

    // TODO: get statistics from storage system
    fn estimated_cardinality(&self) -> usize {
        1
    }

    fn prune_col(&self, required_cols: BitSet) -> PlanRef {
        let (column_ids, column_descs) = required_cols
            .iter()
            .map(|id| {
                (
                    self.column_ids[id as usize],
                    self.column_descs[id as usize].clone(),
                )
            })
            .unzip();
        Self {
            table_ref_id: self.table_ref_id,
            column_ids,
            column_descs,
            with_row_handler: self.with_row_handler,
            is_sorted: self.is_sorted,
            expr: self.expr.clone(),
        }
        .into_plan_ref()
    }
}
impl fmt::Display for LogicalTableScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
                f,
                "LogicalTableScan: table #{}, columns [{}], with_row_handler: {}, is_sorted: {}, expr: {}",
                self.table_ref_id.table_id,
                self.column_ids.iter().map(ToString::to_string).join(", "),
                self.with_row_handler,
                self.is_sorted,
                self.expr.clone().map_or_else(|| "None".to_string(), |expr| format!("{:?}", expr))
            )
    }
}
