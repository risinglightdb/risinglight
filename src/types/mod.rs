use std::any::Any;
use std::sync::Arc;

pub(crate) use self::bool_type::*;
pub(crate) use self::numeric_types::*;

mod bool_type;
mod numeric_types;

/// PostgreSQL DataType
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PgSQLDataTypeEnum {
    Integer,
    Boolean,
    Double,
    Char,
}

/// Inner data type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DataTypeEnum {
    Int32,
    Bool,
    Float64,
    Char,
}

pub(crate) trait DataType: Send + Sync + 'static {
    fn is_nullable(&self) -> bool;
    fn get_type(&self) -> DataTypeEnum;
    fn data_len(&self) -> u32;
    fn as_any(&self) -> &dyn Any;
}

pub(crate) type DataTypeRef = Arc<dyn DataType>;
pub(crate) type DatabaseId = u32;
pub(crate) type SchemaId = u32;
pub(crate) type TableId = u32;
pub(crate) type ColumnId = u32;
