use crate::catalog::TableRefId;
use crate::types::ColumnId;

#[derive(Debug, PartialEq, Clone)]
pub struct SeqScanLogicalPlan {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
}

impl SeqScanLogicalPlan {
    pub fn new(table_ref_id: &TableRefId, column_ids: &[ColumnId]) -> SeqScanLogicalPlan {
        SeqScanLogicalPlan {
            table_ref_id: *table_ref_id,
            column_ids: column_ids.to_vec(),
        }
    }
}
