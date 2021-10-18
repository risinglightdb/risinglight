use crate::catalog::TableRefId;
use crate::types::ColumnId;

/// The logical plan of sequential scan operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalSeqScan {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
}
