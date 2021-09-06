use super::*;
use crate::catalog::TableRefId;
use crate::types::ColumnId;

#[derive(Debug, PartialEq, Clone)]
pub struct SeqScanPlan {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
}

impl SeqScanPlan {
    pub fn new(table_ref_id: &TableRefId, column_ids: &Vec<ColumnId>) -> SeqScanPlan {
        SeqScanPlan {
            table_ref_id: *table_ref_id,
            column_ids: column_ids.to_vec(),
        }
    }
}
