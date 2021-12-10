use crate::{catalog::TableRefId, types::ColumnId};

/// The logical plan of sequential scan operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalSeqScan {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub with_row_handler: bool,
    pub is_sorted: bool,
}
