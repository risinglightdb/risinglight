use std::fmt;

use itertools::Itertools;

use super::*;
use crate::catalog::TableRefId;
use crate::types::ColumnId;

/// The physical plan of `INSERT`.
#[derive(Debug, Clone)]
pub struct PhysicalInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub child: PlanRef,
}

impl_plan_tree_node!(PhysicalInsert, [child]);
impl PlanNode for PhysicalInsert {}

impl fmt::Display for PhysicalInsert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalInsert: table {}, columns [{}]",
            self.table_ref_id.table_id,
            self.column_ids.iter().map(ToString::to_string).join(", ")
        )
    }
}
