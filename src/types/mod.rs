// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::hash::Hash;
use std::num::ParseIntError;

use num_traits::ToPrimitive;
use ordered_float::OrderedFloat;
use parse_display::Display;
use rust_decimal::prelude::FromStr;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

mod blob;
mod date;
mod interval;
mod native;

pub use self::blob::*;
pub use self::date::*;
pub use self::interval::*;
pub use self::native::*;

/// Physical data type
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DataTypeKind {
    Int32,
    Int64,
    // Float32,
    Float64,
    String,
    Blob,
    Bool,
    // decimal (precision, scale)
    Decimal(Option<u8>, Option<u8>),
    Date,
    Interval,
}

impl DataTypeKind {
    pub const fn is_number(&self) -> bool {
        matches!(
            self,
            Self::Int32 | Self::Int64 | Self::Float64 | Self::Decimal(_, _)
        )
    }
}

impl From<&crate::parser::DataType> for DataTypeKind {
    fn from(kind: &crate::parser::DataType) -> Self {
        use crate::parser::DataType::*;
        match kind {
            Char(_) | Varchar(_) | String => Self::String,
            Bytea | Binary(_) | Varbinary(_) | Blob(_) => Self::Blob,
            // Real => Self::Float32,
            Float(_) | Double => Self::Float64,
            Int(_) => Self::Int32,
            BigInt(_) => Self::Int64,
            Boolean => Self::Bool,
            Decimal(p, s) => Self::Decimal(p.map(|x| x as u8), s.map(|x| x as u8)),
            Date => Self::Date,
            Interval => Self::Interval,
            _ => todo!("not supported type: {:?}", kind),
        }
    }
}

impl std::fmt::Display for DataTypeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
        self.kind
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

pub(crate) type DatabaseId = u32;
pub(crate) type SchemaId = u32;
pub(crate) type TableId = u32;
pub(crate) type ColumnId = u32;

/// Primitive SQL value.
#[derive(Debug, Display, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum DataValue {
    // NOTE: Null comes first.
    // => NULL is less than any non-NULL values
    #[display("null")]
    Null,
    #[display("{0}")]
    Bool(bool),
    #[display("{0}")]
    Int32(i32),
    #[display("{0}")]
    Int64(i64),
    #[display("{0}")]
    Float64(F64),
    #[display("'{0}'")]
    String(String),
    #[display("{0}")]
    Blob(Blob),
    #[display("{0}")]
    Decimal(Decimal),
    #[display("{0}")]
    Date(Date),
    #[display("{0}")]
    Interval(Interval),
}

/// A wrapper around floats providing implementations of `Eq`, `Ord`, and `Hash`.
pub type F32 = OrderedFloat<f32>;
pub type F64 = OrderedFloat<f64>;

macro_rules! impl_arith_for_datavalue {
    ($Trait:ident, $name:ident) => {
        impl std::ops::$Trait for &DataValue {
            type Output = DataValue;

            fn $name(self, rhs: Self) -> Self::Output {
                use DataValue::*;
                match (self, rhs) {
                    (&Int32(x), &Int32(y)) => Int32(x.$name(y)),
                    (&Int64(x), &Int64(y)) => Int64(x.$name(y)),
                    (&Float64(x), &Float64(y)) => Float64(x.$name(y)),
                    (&Decimal(x), &Decimal(y)) => Decimal(x.$name(y)),
                    (&Date(x), &Interval(y)) => Date(x.$name(y)),
                    _ => panic!(
                        "invalid operation: {:?} {} {:?}",
                        self,
                        stringify!($name),
                        rhs
                    ),
                }
            }
        }
    };
}
impl_arith_for_datavalue!(Add, add);
impl_arith_for_datavalue!(Sub, sub);
impl_arith_for_datavalue!(Mul, mul);
impl_arith_for_datavalue!(Div, div);
impl_arith_for_datavalue!(Rem, rem);

impl DataValue {
    /// Returns `true` if value is null.
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Whether the value is divisible by another.
    pub fn is_divisible_by(&self, other: &DataValue) -> bool {
        use DataValue::*;
        match (self, other) {
            (&Int32(x), &Int32(y)) => y != 0 && x % y == 0,
            (&Int64(x), &Int64(y)) => y != 0 && x % y == 0,
            (&Float64(x), &Float64(y)) => y != 0.0 && x % y == 0.0,
            (&Decimal(x), &Decimal(y)) => !y.is_zero() && (x % y).is_zero(),
            _ => false,
        }
    }

    /// Returns `true` if value is positive and `false` if the number is zero or negative.
    pub fn is_positive(&self) -> bool {
        match self {
            Self::Null => false,
            Self::Bool(v) => *v,
            Self::Int32(v) => v.is_positive(),
            Self::Int64(v) => v.is_positive(),
            Self::Float64(v) => v.0.is_sign_positive(),
            Self::String(_) => false,
            Self::Blob(_) => false,
            Self::Decimal(v) => v.is_sign_positive(),
            Self::Date(_) => false,
            Self::Interval(v) => v.is_positive(),
        }
    }

    /// Returns `true` if value is zero.
    pub fn is_zero(&self) -> bool {
        match self {
            Self::Null => false,
            Self::Bool(v) => !*v,
            Self::Int32(v) => *v == 0,
            Self::Int64(v) => *v == 0,
            Self::Float64(v) => v.0 == 0.0,
            Self::String(_) => false,
            Self::Blob(_) => false,
            Self::Decimal(v) => v.is_zero(),
            Self::Date(_) => false,
            Self::Interval(v) => v.is_zero(),
        }
    }

    /// Get the type of value. `None` means NULL.
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Bool(_) => Some(DataTypeKind::Bool.not_null()),
            Self::Int32(_) => Some(DataTypeKind::Int32.not_null()),
            Self::Int64(_) => Some(DataTypeKind::Int64.not_null()),
            Self::Float64(_) => Some(DataTypeKind::Float64.not_null()),
            Self::String(_) => Some(DataTypeKind::String.not_null()),
            Self::Blob(_) => Some(DataTypeKind::Blob.not_null()),
            Self::Decimal(_) => Some(DataTypeKind::Decimal(None, None).not_null()),
            Self::Date(_) => Some(DataTypeKind::Date.not_null()),
            Self::Interval(_) => Some(DataTypeKind::Interval.not_null()),
            Self::Null => None,
        }
    }

    /// Convert the value to a usize.
    pub fn as_usize(&self) -> Result<Option<usize>, ConvertError> {
        let cast_err = || ConvertError::Cast(self.to_string(), "usize");
        Ok(Some(match self {
            Self::Null => return Ok(None),
            &Self::Bool(b) => b as usize,
            &Self::Int32(v) => v.try_into().map_err(|_| cast_err())?,
            &Self::Int64(v) => v.try_into().map_err(|_| cast_err())?,
            &Self::Float64(f) if f.is_sign_negative() => return Err(cast_err()),
            &Self::Float64(f) => f.0.to_usize().ok_or_else(cast_err)?,
            &Self::Decimal(d) if d.is_sign_negative() => return Err(cast_err()),
            &Self::Decimal(d) => d.to_usize().ok_or_else(cast_err)?,
            &Self::Date(_) => return Err(cast_err()),
            &Self::Interval(_) => return Err(cast_err()),
            Self::String(s) => s.parse::<usize>().map_err(|_| cast_err())?,
            Self::Blob(_) => return Err(cast_err()),
        }))
    }
}

/// The error type of value type convention.
#[derive(thiserror::Error, Debug, Clone, PartialEq)]
#[allow(named_arguments_used_positionally)]
pub enum ConvertError {
    #[error("failed to convert string {0:?} to int: {1:?}")]
    ParseInt(String, #[source] std::num::ParseIntError),
    #[error("failed to convert string {0:?} to float: {1:?}")]
    ParseFloat(String, #[source] std::num::ParseFloatError),
    #[error("failed to convert string {0:?} to bool: {1:?}")]
    ParseBool(String, #[source] std::str::ParseBoolError),
    #[error("failed to convert string {0:?} to decimal: {1:?}")]
    ParseDecimal(String, #[source] rust_decimal::Error),
    #[error("failed to convert string {0:?} to date: {1:?}")]
    ParseDate(String, #[source] chrono::ParseError),
    #[error("failed to convert string {0:?} to interval")]
    ParseInterval(String),
    #[error("failed to convert string {0:?} to blob: {1:?}")]
    ParseBlob(String, #[source] ParseBlobError),
    #[error("failed to convert {0:?} to decimal")]
    ToDecimalError(DataValue),
    #[error("failed to convert {0:?} from decimal {1:?}")]
    FromDecimalError(DataTypeKind, DataValue),
    #[error("failed to convert {0:?} to date")]
    ToDateError(DataTypeKind),
    #[error("failed to convert {0:?} from date")]
    FromDateError(DataTypeKind),
    #[error("failed to convert {0:?} from interval")]
    FromIntervalError(DataTypeKind),
    #[error("failed to cast {0} to type {1}")]
    Cast(String, &'static str),
    #[error("constant {0:?} overflows {1:?}")]
    Overflow(DataValue, DataTypeKind),
    #[error("no function {0}({1})")]
    NoUnaryOp(String, &'static str),
    #[error("no function {0}({1}, {2})")]
    NoBinaryOp(String, &'static str, &'static str),
}

/// memory table row type
pub(crate) type Row = Vec<DataValue>;

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ParseValueError {
    #[error("invalid interval: {0}")]
    ParseIntervalError(#[from] ParseIntervalError),
    #[error("invalid date: {0}")]
    ParseDateError(#[from] ParseDateError),
    #[error("invalid blob: {0}")]
    ParseBlobError(#[from] ParseBlobError),
    #[error("invalid value: {0}")]
    Invalid(String),
}

impl FromStr for DataValue {
    type Err = ParseValueError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "null" {
            Ok(DataValue::Null)
        } else if let Ok(bool) = s.parse::<bool>() {
            Ok(Self::Bool(bool))
        } else if let Ok(int) = s.parse::<i32>() {
            Ok(Self::Int32(int))
        } else if let Ok(bigint) = s.parse::<i64>() {
            Ok(Self::Int64(bigint))
        } else if let Ok(d) = s.parse::<Decimal>() {
            Ok(Self::Decimal(d))
        } else if s.starts_with('\'') && s.ends_with('\'') {
            Ok(Self::String(s[1..s.len() - 1].to_string()))
        } else if s.starts_with("b\'") && s.ends_with('\'') {
            Ok(Self::Blob(s[2..s.len() - 1].parse()?))
        } else if let Some(s) = s.strip_prefix("interval") {
            Ok(Self::Interval(s.trim().trim_matches('\'').parse()?))
        } else if let Some(s) = s.strip_prefix("date") {
            Ok(Self::Date(s.trim().trim_matches('\'').parse()?))
        } else {
            Err(ParseValueError::Invalid(s.into()))
        }
    }
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
