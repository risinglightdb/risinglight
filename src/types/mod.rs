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
    typeinfo: DataTypeEnum,
    nullable: bool,
}

impl DataType {
    pub fn new(typeinfo: DataTypeEnum, nullable: bool) -> DataType {
        DataType {
            typeinfo: typeinfo,
            nullable: nullable,
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn data_type_info(&self) -> DataTypeEnum {
        self.typeinfo
    }
}

/// Inner data type
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataTypeEnum {
    Int32,
    Bool,
    Float64,
    Char,
}

impl FromStr for DataTypeEnum {
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

impl ToString for DataTypeEnum {
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

impl DataTypeEnum {
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

impl DataValue {
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Bool(_) => Some(DataType::new(DataTypeEnum::Bool, false)),
            Self::Int32(_) => Some(DataType::new(DataTypeEnum::Int32, false)),
            Self::Float64(_) => Some(DataType::new(DataTypeEnum::Float64, false)),
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
