use serde::{Deserialize, Serialize};

pub use sqlparser::ast::DataType as DataTypeKind;

mod native;
pub(crate) use native::*;
use std::hash::{Hash, Hasher};

/// Data type with nullable.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DataType {
    pub kind: DataTypeKind,
    pub nullable: bool,
}

impl std::fmt::Debug for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.kind)?;
        if self.nullable {
            write!(f, " (null)")?;
        }
        Ok(())
    }
}

impl DataType {
    pub const fn new(kind: DataTypeKind, nullable: bool) -> DataType {
        DataType { kind, nullable }
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn kind(&self) -> DataTypeKind {
        self.kind.clone()
    }
}

/// The extension methods for [`DataType`].
pub trait DataTypeExt {
    fn nullable(self) -> DataType;
    fn not_null(self) -> DataType;
}

impl DataTypeExt for DataTypeKind {
    fn nullable(self) -> DataType {
        DataType::new(self, true)
    }

    fn not_null(self) -> DataType {
        DataType::new(self, false)
    }
}

// const CHAR_DEFAULT_LEN: u64 = 1;
const VARCHAR_DEFAULT_LEN: u64 = 256;

pub(crate) type DatabaseId = u32;
pub(crate) type SchemaId = u32;
pub(crate) type TableId = u32;
pub(crate) type ColumnId = u32;

/// Primitive SQL value.
#[derive(Debug, Clone, PartialOrd)]
pub enum DataValue {
    // NOTE: Null comes first.
    // => NULL is less than any non-NULL values
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    String(String),
}

impl PartialEq for DataValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(left), Self::Bool(right)) => left == right,
            (Self::Int32(left), Self::Int32(right)) => left == right,
            (Self::Int64(left), Self::Int64(right)) => left == right,
            (Self::String(left), Self::String(right)) => left == right,
            (Self::Float64(left), Self::Float64(right)) => left == right,
            _ => false,
        }
    }
}
impl Eq for DataValue {}
impl Hash for DataValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Bool(b) => b.hash(state),
            Self::Int32(i) => i.hash(state),
            Self::Int64(i) => i.hash(state),
            Self::String(s) => s.hash(state),
            // TODO: support `f64` as hash key (group key)
            _ => panic!("Unsupported data type"),
        }
    }
}

impl DataValue {
    /// Get the type of value. `None` means NULL.
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Bool(_) => Some(DataTypeKind::Boolean.not_null()),
            Self::Int32(_) => Some(DataTypeKind::Int(None).not_null()),
            Self::Int64(_) => Some(DataTypeKind::BigInt(None).not_null()),
            Self::Float64(_) => Some(DataTypeKind::Double.not_null()),
            Self::String(_) => Some(DataTypeKind::Varchar(Some(VARCHAR_DEFAULT_LEN)).not_null()),
            Self::Null => None,
        }
    }

    /// Convert the value to a usize.
    pub fn as_usize(&self) -> Result<Option<usize>, ConvertError> {
        Ok(Some(match self {
            DataValue::Null => return Ok(None),
            &DataValue::Bool(b) => b as usize,
            &DataValue::Int32(v) => v
                .try_into()
                .map_err(|_| ConvertError::Cast(v.to_string(), "usize"))?,
            &DataValue::Int64(v) => v
                .try_into()
                .map_err(|_| ConvertError::Cast(v.to_string(), "usize"))?,
            &DataValue::Float64(f) if f.is_sign_negative() => {
                return Err(ConvertError::Cast(f.to_string(), "usize"));
            }
            &DataValue::Float64(f) => f as usize,
            DataValue::String(s) => s
                .parse::<usize>()
                .map_err(|e| ConvertError::ParseInt(s.clone(), e))?,
        }))
    }
}

/// The error type of value type convention.
#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum ConvertError {
    #[error("failed to convert string {0:?} to int: {:?}")]
    ParseInt(String, std::num::ParseIntError),
    #[error("failed to convert string {0:?} to float: {:?}")]
    ParseFloat(String, std::num::ParseFloatError),
    #[error("failed to convert string {0:?} to bool: {:?}")]
    ParseBool(String, std::str::ParseBoolError),
    #[error("failed to cast {0} to type {1}")]
    Cast(String, &'static str),
}

/// memory table row type
pub(crate) type Row = Vec<DataValue>;
