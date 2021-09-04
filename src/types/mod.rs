use std::str::FromStr;

/// PostgreSQL DataType
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PgSQLDataTypeEnum {
    Integer,
    Boolean,
    Double,
    Char,
}

/// Inner data type
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DataType {
    Int32,
    Bool,
    Float64,
    Char,
}

impl FromStr for DataType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "int" | "int4" | "signed" | "integer" | "intergral" | "int32" => Ok(Self::Int32),
            "bool" | "boolean" | "logical" => Ok(Self::Bool),
            "double" | "float8" => Ok(Self::Float64),
            "char" | "bpchar" => Ok(Self::Char),
            _ => todo!("parse datatype"),
        }
    }
}

impl ToString for DataType {
    fn to_string(&self) -> String {
        match self {
            Self::Int32 => "INTEGER",
            Self::Bool => "BOOLEAN",
            Self::Float64 => "DOUBLE",
            Self::Char => "CHAR",
        }
        .into()
    }
}

impl DataType {
    pub const fn data_len(&self) -> usize {
        use std::mem::size_of;
        match self {
            Self::Int32 => size_of::<i32>(),
            Self::Bool => size_of::<bool>(),
            Self::Float64 => size_of::<f64>(),
            Self::Char => size_of::<u8>(),
        }
    }
}

pub(crate) type DatabaseId = u32;
pub(crate) type SchemaId = u32;
pub(crate) type TableId = u32;
pub(crate) type ColumnId = u32;

#[derive(Debug, Clone, PartialEq)]
pub enum DataValue {
    Null,
    Bool(bool),
    Int32(i32),
    Float64(f64),
    String(String),
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinType {
    Left,
    Right,
    Inner,
    Outer,
    Semi,
}
