use std::fmt;

use itertools::Itertools;

use super::*;
use crate::catalog::{ColumnDesc, TableRefId};
use crate::types::ColumnId;

/// The physical plan of table scan operation.
#[derive(Debug, Clone)]
pub struct PhysicalTableScan {
    logical: LogicalTableScan,
}

impl PhysicalTableScan {
    pub fn new(logical: LogicalTableScan) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical table scan's logical.
    pub fn logical(&self) -> &LogicalTableScan {
        &self.logical
    }
}

impl PlanTreeNodeLeaf for PhysicalTableScan {}
impl_plan_tree_node_for_leaf!(PhysicalTableScan);
impl PlanNode for PhysicalTableScan {
    fn out_types(&self) -> Vec<DataType> {
        return self.out_types();
    }
}

impl fmt::Display for PhysicalTableScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalTableScan: table #{}, columns [{}], with_row_handler: {}, is_sorted: {}, expr: {}",
            self.logical().table_ref_id().table_id,
            self.logical().column_ids().iter().map(ToString::to_string).join(", "),
            self.logical().with_row_handler(),
            self.logical().is_sorted(),
            self.logical().expr().clone().map_or_else(|| "None".to_string(), |expr| format!("{:?}", expr))
        )
    }
}