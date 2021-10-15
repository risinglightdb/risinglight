use crate::catalog::TableRefId;
use crate::types::ColumnId;

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalSeqScan {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
}
