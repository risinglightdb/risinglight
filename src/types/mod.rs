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
    Null,
    Bool,
    Int32,
    Float64,
    Char(u32),
    Varchar(u32),
}

impl DataTypeKind {
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

const CHAR_DEFAULT_LEN: u32 = 1;
const VARCHAR_DEFAULT_LEN: u32 = 256;

impl FromStr for DataTypeKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "int" | "int4" | "signed" | "integer" | "intergral" | "int32" => Ok(Self::Int32),
            "bool" | "boolean" | "logical" => Ok(Self::Bool),
            "double" | "float8" => Ok(Self::Float64),
            "char" | "bpchar" => Ok(Self::Char(CHAR_DEFAULT_LEN)),
            "varchar" | "text" | "string" => Ok(Self::Varchar(VARCHAR_DEFAULT_LEN)),
            _ => todo!("parse datatype"),
        }
    }
}

impl ToString for DataTypeKind {
    fn to_string(&self) -> String {
        match self {
            Self::Null => "NULL",
            Self::Int32 => "INTEGER",
            Self::Bool => "BOOLEAN",
            Self::Float64 => "DOUBLE",
            Self::Char(_) => "CHAR",
            Self::Varchar(_) => "VARCHAR",
        }
        .into()
    }
}

impl DataTypeKind {
    pub const fn data_len(&self) -> usize {
        use std::mem::size_of;
        match self {
            Self::Null => size_of::<()>(),
            Self::Int32 => size_of::<i32>(),
            Self::Bool => size_of::<bool>(),
            Self::Float64 => size_of::<f64>(),
            Self::Char(len) => *len as _,
            Self::Varchar(len) => *len as _,
        }
    }

    pub const fn nullable(self) -> DataType {
        DataType::new(self, true)
    }

    pub const fn not_null(self) -> DataType {
        DataType::new(self, false)
    }

    pub fn set_len(&mut self, len: u32) {
        match self {
            Self::Char(l) => *l = len,
            Self::Varchar(l) => *l = len,
            _ => {}
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
    pub const fn data_type(&self) -> DataType {
        match self {
            Self::Bool(_) => DataTypeKind::Bool.not_null(),
            Self::Int32(_) => DataTypeKind::Int32.not_null(),
            Self::Float64(_) => DataTypeKind::Float64.not_null(),
            Self::String(_) => DataTypeKind::Varchar(VARCHAR_DEFAULT_LEN).not_null(),
            Self::Null => DataTypeKind::Null.nullable(),
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
