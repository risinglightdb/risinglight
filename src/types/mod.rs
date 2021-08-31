use std::any::Any;
use std::sync::Arc;
// PostgreSQL DataType
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub(crate) enum PgSQLDataTypeEnum {
    Integer,
    Boolean,
    Double,
    Char,
}

// Inner data type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub(crate) enum DataTypeEnum {
    Int32,
    Bool,
    Float64,
    Char,
}

pub(crate) trait DataType {
    fn is_nullable(&self) -> bool;
    fn get_type() -> DataTypeEnum;
    fn get_data_len() -> u32;
    fn as_any(&self) -> &dyn Any;
}

pub(crate) type DataTypeRef = Arc<dyn DataType>;

mod numeric_types;
pub(crate) use numeric_types::*;
