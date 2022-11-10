use num_traits::ToPrimitive;
use ordered_float::OrderedFloat;
use parse_display::Display;
use rust_decimal::Decimal;
use serde::Serialize;

use super::*;
use crate::array::ArrayImpl;
use crate::for_all_variants_without_null;

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

/// memory table row type
pub type Row = Vec<DataValue>;

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
                    (&Null, _) | (_, &Null) => Null,
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

        impl std::ops::$Trait for DataValue {
            type Output = DataValue;
            fn $name(self, rhs: Self) -> Self::Output {
                (&self).$name(&rhs)
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

    /// Get the type of value.
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataTypeKind::Null.nullable(),
            Self::Bool(_) => DataTypeKind::Bool.not_null(),
            Self::Int32(_) => DataTypeKind::Int32.not_null(),
            Self::Int64(_) => DataTypeKind::Int64.not_null(),
            Self::Float64(_) => DataTypeKind::Float64.not_null(),
            Self::String(_) => DataTypeKind::String.not_null(),
            Self::Blob(_) => DataTypeKind::Blob.not_null(),
            Self::Decimal(_) => DataTypeKind::Decimal(None, None).not_null(),
            Self::Date(_) => DataTypeKind::Date.not_null(),
            Self::Interval(_) => DataTypeKind::Interval.not_null(),
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

    /// Cast the value to another type.
    pub fn cast(&self, ty: &DataTypeKind) -> Result<Self, ConvertError> {
        Ok(ArrayImpl::from(self).cast(ty)?.get(0))
    }
}

/// Implement aggregation functions.
macro_rules! impl_min_max {
    ([], $( { $Abc:ident, $Type:ty, $abc:ident, $AbcArray:ty, $AbcArrayBuilder:ty, $Value:ident, $Pattern:pat } ),*) => {
    $(
        impl From<Option<&$Type>> for DataValue {
            fn from(v: Option<&$Type>) -> Self {
                match v {
                    Some(v) => Self::$Value(v.to_owned()),
                    None => Self::Null,
                }
            }
        }
    )*

        impl DataValue {
            /// Compares and returns the minimum of two values.
            pub fn min(self, other: Self) -> Self {
                match (self, other) {
                    (Self::Null, a) | (a, Self::Null) => a.clone(),
                    $(
                        (Self::$Value(a), Self::$Value(b)) => Self::$Value(a.min(b)),
                    )*
                    (a, b) => panic!("invalid operation: min({a:?}, {b:?})"),
                }
            }

            /// Compares and returns the minimum of two values.
            pub fn max(self, other: Self) -> Self {
                match (self, other) {
                    (Self::Null, a) | (a, Self::Null) => a.clone(),
                    $(
                        (Self::$Value(a), Self::$Value(b)) => Self::$Value(a.max(b)),
                    )*
                    (a, b) => panic!("invalid operation: max({a:?}, {b:?})"),
                }
            }
        }
    }
}

for_all_variants_without_null! { impl_min_max }

impl From<Option<&()>> for DataValue {
    fn from(_: Option<&()>) -> Self {
        Self::Null
    }
}

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
