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
            "cahr" | "bpchar" => Ok(Self::Char),
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
    pub fn data_len(&self) -> usize {
        match self {
            Self::Int32 => 4,
            Self::Bool => 1,
            Self::Float64 => 8,
            Self::Char => 1,
        }
    }
}

pub(crate) type DatabaseId = u32;
pub(crate) type SchemaId = u32;
pub(crate) type TableId = u32;
pub(crate) type ColumnId = u32;
