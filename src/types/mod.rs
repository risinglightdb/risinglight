use std::str::FromStr;

mod native;
pub(crate) use native::*;

/// PostgreSQL DataTypeEnum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PgSQLDataTypeEnumEnum {
    Integer,
    Boolean,
    Double,
    Char,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DataType {
    kind: DataTypeKind,
    nullable: bool,
}

impl DataType {
    pub const fn new(kind: DataTypeKind, nullable: bool) -> DataType {
        DataType { kind, nullable }
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn kind(&self) -> DataTypeKind {
        self.kind
    }
}

/// Inner data type
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataTypeKind {
    Int32,
    Bool,
    Float64,
    Char,
    Varchar,
}

impl FromStr for DataTypeKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "int" | "int4" | "signed" | "integer" | "intergral" | "int32" => Ok(Self::Int32),
            "bool" | "boolean" | "logical" => Ok(Self::Bool),
            "double" | "float8" => Ok(Self::Float64),
            "char" | "bpchar" => Ok(Self::Char),
            "varchar" => Ok(Self::Varchar),
            _ => todo!("parse datatype"),
        }
    }
}

impl ToString for DataTypeKind {
    fn to_string(&self) -> String {
        match self {
            Self::Int32 => "INTEGER",
            Self::Bool => "BOOLEAN",
            Self::Float64 => "DOUBLE",
            Self::Char => "CHAR",
            Self::Varchar => "VARCHAR",
        }
        .into()
    }
}

impl DataTypeKind {
    pub const fn data_len(&self) -> usize {
        use std::mem::size_of;
        match self {
            Self::Int32 => size_of::<i32>(),
            Self::Bool => size_of::<bool>(),
            Self::Float64 => size_of::<f64>(),
            Self::Char => size_of::<u8>(),
            Self::Varchar => size_of::<u8>(),
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

impl DataValue {
    pub const fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Bool(_) => Some(DataType::new(DataTypeKind::Bool, false)),
            Self::Int32(_) => Some(DataType::new(DataTypeKind::Int32, false)),
            Self::Float64(_) => Some(DataType::new(DataTypeKind::Float64, false)),
            Self::String(_) => Some(DataType::new(DataTypeKind::Varchar, false)),
            _ => None,
        }
    }
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
