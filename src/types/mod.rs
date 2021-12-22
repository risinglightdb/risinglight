use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
pub use sqlparser::ast::DataType as DataTypeKind;

mod date;
mod native;
use std::hash::{Hash, Hasher};

pub(crate) use date::*;
pub(crate) use native::*;
use num_traits::ToPrimitive;
use rust_decimal::prelude::FromStr;

/// Physical data type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PhysicalDataTypeKind {
    Int32,
    Int64,
    Float64,
    String,
    Bool,
    Decimal,
    Date,
}

impl From<DataTypeKind> for PhysicalDataTypeKind {
    fn from(kind: DataTypeKind) -> Self {
        match kind {
            DataTypeKind::Char(_) | DataTypeKind::Varchar(_) | DataTypeKind::String => {
                PhysicalDataTypeKind::String
            }
            DataTypeKind::Float(_) | DataTypeKind::Double => PhysicalDataTypeKind::Float64,
            DataTypeKind::Int(_) => PhysicalDataTypeKind::Int32,
            DataTypeKind::BigInt(_) => PhysicalDataTypeKind::Int64,
            DataTypeKind::Boolean => PhysicalDataTypeKind::Bool,
            DataTypeKind::Decimal(_, _) => PhysicalDataTypeKind::Decimal,
            DataTypeKind::Date => PhysicalDataTypeKind::Date,
            _ => todo!("physical type for {:?} is not supported", kind),
        }
    }
}

/// Data type with nullable.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DataType {
    pub kind: DataTypeKind,
    pub physical_kind: PhysicalDataTypeKind,
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
    pub fn new(kind: DataTypeKind, nullable: bool) -> DataType {
        let physical_kind = kind.clone().into();
        DataType {
            kind,
            physical_kind,
            nullable,
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn kind(&self) -> DataTypeKind {
        self.kind.clone()
    }

    /// Get physical data type
    pub fn physical_kind(&self) -> PhysicalDataTypeKind {
        self.physical_kind.clone()
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
    Decimal(Decimal),
    Date(Date),
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
            (Self::Decimal(left), Self::Decimal(right)) => left == right,
            (Self::Date(left), Self::Date(right)) => left == right,
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

macro_rules! impl_arith_for_datavalue {
    ($Trait:ident, $name:ident) => {
        impl std::ops::$Trait for &DataValue {
            type Output = DataValue;

            fn $name(self, rhs: Self) -> Self::Output {
                use DataValue::*;
                match (self, rhs) {
                    (&Int32(x), &Int32(y)) => Int32(x.$name(y)),
                    (&Float64(x), &Float64(y)) => Float64(x.$name(y)),
                    (&Decimal(x), &Decimal(y)) => Decimal(x.$name(y)),
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
    /// Whether the value is divisible by another.
    pub fn is_divisible_by(&self, other: &DataValue) -> bool {
        use DataValue::*;
        match (self, other) {
            (&Int32(x), &Int32(y)) => y != 0 && x % y == 0,
            (&Float64(x), &Float64(y)) => y != 0.0 && x % y == 0.0,
            (&Decimal(x), &Decimal(y)) => {
                y != rust_decimal::Decimal::from_str("0.0").unwrap()
                    && x % y == rust_decimal::Decimal::from_str("0.0").unwrap()
            }
            _ => false,
        }
    }

    /// Returns `true` if value is positive and `false` if the number is zero or negative.
    pub fn is_positive(&self) -> bool {
        match self {
            Self::Bool(v) => *v,
            Self::Int32(v) => v.is_positive(),
            Self::Int64(v) => v.is_positive(),
            Self::Float64(v) => v.is_sign_positive(),
            Self::String(_) => false,
            Self::Decimal(v) => v.is_sign_positive(),
            Self::Date(_) => false,
            Self::Null => false,
        }
    }

    /// Get the type of value. `None` means NULL.
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Bool(_) => Some(DataTypeKind::Boolean.not_null()),
            Self::Int32(_) => Some(DataTypeKind::Int(None).not_null()),
            Self::Int64(_) => Some(DataTypeKind::BigInt(None).not_null()),
            Self::Float64(_) => Some(DataTypeKind::Double.not_null()),
            Self::String(_) => Some(DataTypeKind::Varchar(Some(VARCHAR_DEFAULT_LEN)).not_null()),
            Self::Decimal(_) => Some(DataTypeKind::Decimal(None, None).not_null()),
            Self::Date(_) => Some(DataTypeKind::Date.not_null()),
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
            &DataValue::Decimal(d) if d.is_sign_negative() => {
                return Err(ConvertError::Cast(d.to_string(), "usize"));
            }
            &DataValue::Decimal(d) => d.to_f64().ok_or(ConvertError::FromDecimalError(
                DataTypeKind::Double,
                DataValue::Decimal(d),
            ))? as usize,
            &DataValue::Date(d) => {
                return Err(ConvertError::Cast(d.to_string(), "usize"));
            }
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
    #[error("failed to convert string {0:?} to decimal: {:?}")]
    ParseDecimal(String, rust_decimal::Error),
    #[error("failed to convert string {0:?} to date: {:?}")]
    ParseDate(String, chrono::ParseError),
    #[error("failed to convert {0:?} to decimal")]
    ToDecimalError(DataValue),
    #[error("failed to convert {0:?} from decimal {1:?}")]
    FromDecimalError(DataTypeKind, DataValue),
    #[error("failed to convert {0:?} to date")]
    ToDateError(DataTypeKind),
    #[error("failed to convert {0:?} from date")]
    FromDateError(DataTypeKind),
    #[error("failed to cast {0} to type {1}")]
    Cast(String, &'static str),
}

/// memory table row type
pub(crate) type Row = Vec<DataValue>;
