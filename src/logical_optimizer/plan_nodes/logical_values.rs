use crate::binder::BoundExpr;
use crate::types::DataType;

/// The logical plan of `VALUES`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalValues {
    pub column_types: Vec<DataType>,
    pub values: Vec<Vec<BoundExpr>>,
}
