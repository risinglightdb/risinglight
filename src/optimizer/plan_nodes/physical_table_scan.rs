// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use indoc::indoc;
use itertools::Itertools;
use serde::Serialize;

use super::*;

/// The physical plan of table scan operation.
#[derive(Debug, Clone, Serialize)]
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
    fn schema(&self) -> Vec<ColumnDesc> {
        self.logical().schema()
    }
    // TODO: get statistics from storage system
    fn estimated_cardinality(&self) -> usize {
        1
    }
}

impl fmt::Display for PhysicalTableScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            indoc! {"
			PhysicalTableScan:
			  table #{},
			  columns [{}],
			  with_row_handler: {},
			  is_sorted: {},
			  expr: {}"},
            self.logical().table_ref_id().table_id,
            self.logical()
                .column_ids()
                .iter()
                .map(ToString::to_string)
                .join(", "),
            self.logical().with_row_handler(),
            self.logical().is_sorted(),
            self.logical()
                .expr()
                .map_or_else(|| "None".to_string(), |expr| format!("{:?}", expr))
        )
    }
}
