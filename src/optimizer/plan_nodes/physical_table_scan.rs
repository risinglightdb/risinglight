use std::fmt;

use itertools::Itertools;

use super::*;
use crate::catalog::{ColumnDesc, TableRefId};
use crate::types::ColumnId;

/// The physical plan of table scan operation.
#[derive(Debug, Clone)]
pub struct PhysicalTableScan {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub column_descs: Vec<ColumnDesc>,
    pub with_row_handler: bool,
    pub is_sorted: bool,
    pub expr: Option<BoundExpr>,
}

impl_plan_tree_node!(PhysicalTableScan);
impl PlanNode for PhysicalTableScan {
    fn out_types(&self) -> Vec<DataType> {
        return self
            .column_descs
            .iter()
            .map(|desc| desc.datatype().clone())
            .collect();
    }
}

impl fmt::Display for PhysicalTableScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.expr {
            Some(expr) => writeln!(
            f,
            "PhysicalTableScan: table #{}, columns [{}], with_row_handler: {}, is_sorted: {}, expr: {:?}",
            self.table_ref_id.table_id,
            self.column_ids.iter().map(ToString::to_string).join(", "),
            self.with_row_handler,
            self.is_sorted,
            expr
        ),
            None => writeln!(
            f,
            "PhysicalTableScan: table #{}, columns [{}], with_row_handler: {}, is_sorted: {}, expr: None",
            self.table_ref_id.table_id,
            self.column_ids.iter().map(ToString::to_string).join(", "),
            self.with_row_handler,
            self.is_sorted
        )
        }
    }
}
