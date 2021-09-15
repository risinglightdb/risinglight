use super::*;
use crate::catalog::TableRefId;
use crate::parser::Expression;
use crate::types::ColumnId;

#[derive(Debug, PartialEq, Clone)]
pub struct InsertPhysicalPlan {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub values: Vec<Vec<Expression>>,
}
