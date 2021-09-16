use crate::catalog::TableRefId;
use crate::parser::Expression;
use crate::types::ColumnId;

#[derive(Debug, PartialEq, Clone)]
pub struct InsertPhysicalPlan {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    /// The rows to be inserted.
    ///
    /// Each row is composed of multiple values,
    /// each value is represented by an expression.
    pub values: Vec<Vec<Expression>>,
}
