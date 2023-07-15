// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::fmt;

use itertools::Itertools;
use serde::Serialize;

use super::*;
use crate::catalog::{ColumnDesc, ColumnId, TableRefId};
use crate::storage::KeyRange;
use crate::types::DataTypeKind;

/// The logical plan of sequential scan operation.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalTableScan {
    table_ref_id: TableRefId,
    column_ids: Vec<ColumnId>,
    column_descs: Vec<ColumnDesc>,
    with_row_handler: bool,
    is_sorted: bool,
    filter: Option<KeyRange>,
}

impl LogicalTableScan {
    pub fn new(
        table_ref_id: TableRefId,
        column_ids: Vec<ColumnId>,
        column_descs: Vec<ColumnDesc>,
        with_row_handler: bool,
        is_sorted: bool,
        filter: Option<KeyRange>,
    ) -> Self {
        Self {
            table_ref_id,
            column_ids,
            column_descs,
            with_row_handler,
            is_sorted,
            filter,
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
    pub fn filter(&self) -> &Option<KeyRange> {
        &self.filter
    }
}
impl PlanTreeNodeLeaf for LogicalTableScan {}
impl_plan_tree_node_for_leaf!(LogicalTableScan);

impl PlanNode for LogicalTableScan {
    fn schema(&self) -> Vec<ColumnDesc> {
        let mut descs = self.column_descs.clone();
        if self.with_row_handler {
            descs.push(ColumnDesc::new(
                DataType::new(DataTypeKind::Int32, false),
                "row_handler".to_string(),
                false,
            ));
        }
        descs
    }

    // TODO: get statistics from storage system
    fn estimated_cardinality(&self) -> usize {
        1
    }

    fn prune_col(&self, required_cols: BitSet) -> PlanRef {
        let mut filter_cols = BitSet::new();
        if self.filter.is_some() {
            // keep the primary key
            for (idx, col) in self.column_descs.iter().enumerate() {
                if col.is_primary() {
                    filter_cols.insert(idx);
                }
            }
        }

        let mut need_rewrite = false;

        if !filter_cols.is_empty()
            && filter_cols
                .iter()
                .any(|index| !required_cols.contains(index))
        {
            need_rewrite = true;
        }

        let mut idx_table = HashMap::new();
        let (column_ids, column_descs): (Vec<_>, Vec<_>) = required_cols
            .iter()
            .filter(|&id| id < self.column_ids.len())
            .map(|id| {
                idx_table.insert(id, idx_table.len());
                (self.column_ids[id], self.column_descs[id].clone())
            })
            .unzip();

        let new_scan = Self {
            table_ref_id: self.table_ref_id,
            column_ids,
            column_descs: column_descs.clone(),
            with_row_handler: self.with_row_handler,
            is_sorted: self.is_sorted,
            filter: self.filter.clone(),
        }
        .into_plan_ref();

        if need_rewrite {
            let project_expressions = (0..required_cols.len())
                .map(|index| {
                    BoundExpr::InputRef(BoundInputRef {
                        index,
                        return_type: column_descs[index].datatype().clone(),
                    })
                })
                .collect();
            LogicalProjection::new(project_expressions, new_scan).into_plan_ref()
        } else {
            new_scan
        }
    }
}
impl fmt::Display for LogicalTableScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
                f,
                "LogicalTableScan: table #{}, columns [{}], with_row_handler: {}, is_sorted: {}, filter: {}",
                self.table_ref_id.table_id,
                self.column_ids.iter().map(ToString::to_string).join(", "),
                self.with_row_handler,
                self.is_sorted,
                self.filter.clone().map_or_else(|| "None".to_string(), |expr| format!("{:?}", expr))
            )
    }
}
