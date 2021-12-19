use std::fmt;

use itertools::Itertools;

use super::*;
use crate::catalog::TableRefId;
use crate::types::ColumnId;

/// The logical plan of `INSERT`.
#[derive(Debug, Clone)]
pub struct LogicalInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub child: PlanRef,
}

impl_plan_node!(LogicalInsert, [child]);

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
