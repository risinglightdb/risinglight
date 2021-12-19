use std::fmt;

use itertools::Itertools;

use super::*;
use crate::catalog::TableRefId;
use crate::types::ColumnId;
use crate::catalog::ColumnDesc;
/// The logical plan of sequential scan operation.
#[derive(Debug, Clone)]
pub struct LogicalSeqScan {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub column_descs: Vec<ColumnDesc>,
    pub with_row_handler: bool,
    pub is_sorted: bool,
}

impl_plan_node!(LogicalSeqScan);

impl fmt::Display for LogicalSeqScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalSeqScan: table #{}, columns [{}], with_row_handler: {}, is_sorted: {}",
            self.table_ref_id.table_id,
            self.column_ids.iter().map(ToString::to_string).join(", "),
            self.with_row_handler,
            self.is_sorted
        )
    }
}
