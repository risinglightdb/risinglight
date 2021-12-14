use std::fmt;

use itertools::Itertools;

use crate::catalog::TableRefId;
use crate::types::ColumnId;

/// The physical plan of sequential scan operation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalSeqScan {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub with_row_handler: bool,
    pub is_sorted: bool,
}

impl fmt::Display for PhysicalSeqScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalSeqScan: table #{}, columns [{}], with_row_handler: {}, is_sorted: {}",
            self.table_ref_id.table_id,
            self.column_ids.iter().map(ToString::to_string).join(", "),
            self.with_row_handler,
            self.is_sorted
        )
    }
}
