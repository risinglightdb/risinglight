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
    fn get_type(&self) -> DataTypeEnum;
    fn get_data_len(&self) -> u32;
    fn as_any(&self) -> &dyn Any;
}

pub(crate) type DataTypeRef = Arc<dyn DataType>;
pub(crate) type database_id_t = u32;
pub(crate) type schema_id_t = u32;
pub(crate) type table_id_t = u32;
pub(crate) type column_id_t = u32;

mod numeric_types;
pub(crate) use numeric_types::*;
mod bool_type;
pub(crate) use bool_type::*;