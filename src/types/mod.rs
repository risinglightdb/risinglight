// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::hash::Hash;
use std::num::ParseIntError;
use std::str::FromStr;

use parse_display::Display;
use serde::{Deserialize, Serialize};

mod blob;
mod date;
mod interval;
mod native;
mod value;

pub use self::blob::*;
pub use self::date::*;
pub use self::interval::*;
pub use self::native::*;
pub use self::value::*;

/// Data type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DataTypeKind {
    // NOTE: order matters
    Null,
    Bool,
    Int32,
    Int64,
    // Float32,
    Float64,
    // decimal (precision, scale)
    Decimal(Option<u8>, Option<u8>),
    Date,
    Interval,
    String,
    Blob,
    Struct(Vec<DataType>),
}

impl DataTypeKind {
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub const fn is_number(&self) -> bool {
        matches!(
            self,
            Self::Int32 | Self::Int64 | Self::Float64 | Self::Decimal(_, _)
        )
    }

    /// Returns the inner types of the struct.
    pub fn as_struct(&self) -> &[DataType] {
        let Self::Struct(types) = self else { panic!("not a struct: {self}") };
        types
    }

    /// Returns the minimum compatible type of 2 types.
    pub fn union(&self, other: &Self) -> Option<Self> {
        use DataTypeKind::*;
        let (a, b) = if self <= other {
            (self, other)
        } else {
            (other, self)
        }; // a <= b
        match (a, b) {
            (Null, _) => Some(b.clone()),
            (Bool, Bool | Int32 | Int64 | Float64 | Decimal(_, _) | String) => Some(b.clone()),
            (Int32, Int32 | Int64 | Float64 | Decimal(_, _) | String) => Some(b.clone()),
            (Int64, Int64 | Float64 | Decimal(_, _) | String) => Some(b.clone()),
            (Float64, Float64 | Decimal(_, _) | String) => Some(b.clone()),
            (Decimal(_, _), Decimal(_, _) | String) => Some(b.clone()),
            (Date, Date | String) => Some(b.clone()),
            (Interval, Interval | String) => Some(b.clone()),
            (String, String | Blob) => Some(b.clone()),
            (Blob, Blob) => Some(b.clone()),
            (Struct(a), Struct(b)) => {
                if a.len() != b.len() {
                    return None;
                }
                let c = (a.iter().zip(b.iter()))
                    .map(|(a, b)| a.union(b))
                    .try_collect()?;
                Some(Struct(c))
            }
            _ => None,
        }
    }
}

impl From<&crate::parser::DataType> for DataTypeKind {
    fn from(kind: &crate::parser::DataType) -> Self {
        use sqlparser::ast::ExactNumberInfo;

        use crate::parser::DataType::*;
        match kind {
            Char(_) | Varchar(_) | String => Self::String,
            Bytea | Binary(_) | Varbinary(_) | Blob(_) => Self::Blob,
            // Real => Self::Float32,
            Float(_) | Double => Self::Float64,
            Int(_) => Self::Int32,
            BigInt(_) => Self::Int64,
            Boolean => Self::Bool,
            Decimal(info) => match info {
                ExactNumberInfo::None => Self::Decimal(None, None),
                ExactNumberInfo::Precision(p) => Self::Decimal(Some(*p as u8), None),
                ExactNumberInfo::PrecisionAndScale(p, s) => {
                    Self::Decimal(Some(*p as u8), Some(*s as u8))
                }
            },
            Date => Self::Date,
            Interval => Self::Interval,
            _ => todo!("not supported type: {:?}", kind),
        }
    }
}

impl std::fmt::Display for DataTypeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Int32 => write!(f, "INT"),
            Self::Int64 => write!(f, "BIGINT"),
            // Self::Float32 => write!(f, "REAL"),
            Self::Float64 => write!(f, "DOUBLE"),
            Self::String => write!(f, "STRING"),
            Self::Blob => write!(f, "BLOB"),
            Self::Bool => write!(f, "BOOLEAN"),
            Self::Decimal(p, s) => match (p, s) {
                (None, None) => write!(f, "DECIMAL"),
                (Some(p), None) => write!(f, "DECIMAL({p})"),
                (Some(p), Some(s)) => write!(f, "DECIMAL({p},{s})"),
                (None, Some(_)) => panic!("invalid decimal"),
            },
            Self::Date => write!(f, "DATE"),
            Self::Interval => write!(f, "INTERVAL"),
            Self::Struct(types) => {
                write!(f, "STRUCT(")?;
                for t in types.iter().take(1) {
                    write!(f, "{}", t.kind())?;
                }
                for t in types.iter().skip(1) {
                    write!(f, ", {}", t.kind())?;
                }
                write!(f, ")")
            }
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ParseTypeError {
    #[error("invalid number: {0}")]
    ParseIntError(#[from] ParseIntError),
    #[error("invalid type: {0}")]
    Invalid(String),
}

impl FromStr for DataTypeKind {
    type Err = ParseTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use DataTypeKind::*;
        Ok(match s {
            "INT" => Int32,
            "BIGINT" => Int64,
            // "REAL" => Float32,
            "DOUBLE" => Float64,
            "STRING" => String,
            "BLOB" => Blob,
            "BOOLEAN" => Bool,
            "DECIMAL" => Decimal(None, None),
            _ if s.starts_with("DECIMAL") => {
                let para = s
                    .strip_prefix("DECIMAL")
                    .unwrap()
                    .trim_matches(|c: char| c == '(' || c == ')' || c.is_ascii_whitespace());
                match para.split_once(',') {
                    Some((p, s)) => Decimal(Some(p.parse()?), Some(s.parse()?)),
                    None => Decimal(Some(para.parse()?), None),
                }
            }
            "DATE" => Date,
            "INTERVAL" => Interval,
            _ => return Err(ParseTypeError::Invalid(s.to_owned())),
        })
    }
}

/// Data type with nullable.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DataType {
    pub kind: DataTypeKind,
    pub nullable: bool,
}

impl std::fmt::Debug for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)?;
        if self.nullable {
            write!(f, " (nullable)")?;
        }
        Ok(())
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl DataType {
    pub fn new(kind: DataTypeKind, nullable: bool) -> DataType {
        DataType { kind, nullable }
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn kind(&self) -> DataTypeKind {
        self.kind.clone()
    }

    /// Returns the minimum compatible type of 2 types.
    pub fn union(&self, other: &Self) -> Option<Self> {
        Some(DataType {
            kind: self.kind.union(&other.kind)?,
            nullable: self.nullable || other.nullable,
        })
    }
}

impl DataTypeKind {
    pub fn nullable(self) -> DataType {
        DataType::new(self, true)
    }

    pub fn not_null(self) -> DataType {
        DataType::new(self, false)
    }
}

/// The error type of value type convention.
#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum ConvertError {
    #[error("failed to convert string {0:?} to int: {1}")]
    ParseInt(String, #[source] std::num::ParseIntError),
    #[error("failed to convert string {0:?} to float: {1}")]
    ParseFloat(String, #[source] std::num::ParseFloatError),
    #[error("failed to convert string {0:?} to bool: {1}")]
    ParseBool(String, #[source] std::str::ParseBoolError),
    #[error("failed to convert string {0:?} to decimal: {1}")]
    ParseDecimal(String, #[source] rust_decimal::Error),
    #[error("failed to convert string {0:?} to date: {1}")]
    ParseDate(String, #[source] chrono::ParseError),
    #[error("failed to convert string {0:?} to interval: {1}")]
    ParseInterval(String, #[source] ParseIntervalError),
    #[error("failed to convert string {0:?} to blob: {1}")]
    ParseBlob(String, #[source] ParseBlobError),
    #[error("failed to convert {0} to decimal")]
    ToDecimalError(DataValue),
    #[error("failed to convert {0} from decimal {1}")]
    FromDecimalError(DataTypeKind, DataValue),
    #[error("failed to convert {0} from date")]
    FromDateError(DataTypeKind),
    #[error("failed to convert {0} from interval")]
    FromIntervalError(DataTypeKind),
    #[error("failed to cast {0} to type {1}")]
    Cast(String, &'static str),
    #[error("constant {0} overflows {1}")]
    Overflow(DataValue, DataTypeKind),
    #[error("no function {0}({1})")]
    NoUnaryOp(String, &'static str),
    #[error("no function {0}({1}, {2})")]
    NoBinaryOp(String, &'static str, &'static str),
    #[error("no cast {0} -> {1}")]
    NoCast(&'static str, DataTypeKind),
}

/// The physical index to the column from child plan.
///
/// It is equivalent to `InputRef` in the old planner.
#[derive(Debug, Display, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone)]
#[display("#{0}")]
pub struct ColumnIndex(pub u32);

#[derive(thiserror::Error, Debug, Clone)]
#[error("parse column index error: {}")]
pub enum ParseColumnIndexError {
    #[error("no leading '#'")]
    NoLeadingSign,
    #[error("invalid number: {0}")]
    InvalidNum(#[from] std::num::ParseIntError),
}

impl FromStr for ColumnIndex {
    type Err = ParseColumnIndexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let body = s.strip_prefix('#').ok_or(Self::Err::NoLeadingSign)?;
        let num = body.parse()?;
        Ok(Self(num))
    }
}
