use std::fmt::Display;
use serde::{Serialize, Deserialize};

pub use sqlparser::ast::DataType as DataTypeKind;

mod native;
pub(crate) use native::*;

/// Data type with nullable.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DataType {
    pub kind: DataTypeKind,
    pub nullable: bool,
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

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.kind)?;
        if self.nullable {
            write!(f, "(nullable)")?;
        }
        Ok(())
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
#[derive(Debug, Clone, PartialEq, PartialOrd)]
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

impl DataValue {
    /// Get the type of value. `None` means NULL.
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Bool(_) => Some(DataTypeKind::Boolean.not_null()),
            Self::Int32(_) => Some(DataTypeKind::Int.not_null()),
            Self::Int64(_) => Some(DataTypeKind::BigInt.not_null()),
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
