//! Array operations.

use std::borrow::Borrow;
use std::simd::{LaneCount, Simd, SimdElement, SupportedLaneCount};

use num_traits::ToPrimitive;
use rust_decimal::prelude::FromStr;
use rust_decimal::Decimal;

use super::*;
use crate::for_all_variants;
use crate::parser::{BinaryOperator, UnaryOperator};
use crate::types::{Blob, ConvertError, DataTypeKind, DataValue, Date, Interval, NativeType, F64};

type A = ArrayImpl;

impl ArrayImpl {
    pub fn neg(&self) -> Result<Self, ConvertError> {
        Ok(match self {
            A::Int32(a) => A::new_int32(unary_op(a.as_ref(), |v| -v)),
            A::Int64(a) => A::new_int64(unary_op(a.as_ref(), |v| -v)),
            A::Float64(a) => A::new_float64(unary_op(a.as_ref(), |v| -v)),
            A::Decimal(a) => A::new_decimal(unary_op(a.as_ref(), |v| -v)),
            _ => return Err(ConvertError::NoUnaryOp("-".into(), self.type_string())),
        })
    }

    pub fn not(&self) -> Result<Self, ConvertError> {
        Ok(match self {
            A::Bool(a) => A::new_bool(unary_op(a.as_ref(), |b| !b)),
            _ => return Err(ConvertError::NoUnaryOp("not".into(), self.type_string())),
        })
    }

    /// Perform unary operation.
    pub fn unary_op(&self, op: &UnaryOperator) -> Result<ArrayImpl, ConvertError> {
        Ok(match op {
            UnaryOperator::Plus => match self {
                A::Int32(_) | A::Int64(_) | A::Float64(_) | A::Decimal(_) | A::Interval(_) => {
                    self.clone()
                }
                _ => return Err(ConvertError::NoUnaryOp("+".into(), self.type_string())),
            },
            UnaryOperator::Minus => self.neg()?,
            UnaryOperator::Not => self.not()?,
            _ => return Err(ConvertError::NoUnaryOp(op.to_string(), self.type_string())),
        })
    }
}

/// A macro to implement arithmetic operations.
macro_rules! arith {
    ($name:ident, $op:tt) => {
        pub fn $name(
            &self,
            other: &Self,
        ) -> Result<Self, ConvertError> {
        Ok(match (self, other) {
            #[cfg(feature = "simd")]
            (A::Int32(a), A::Int32(b)) => A::new_int32(simd_op::<_, _, _, 32>(a, b, |a, b| a $op b)),
            #[cfg(feature = "simd")]
            (A::Int64(a), A::Int64(b)) => A::new_int64(simd_op::<_, _, _, 64>(a, b, |a, b| a $op b)),
            #[cfg(feature = "simd")]
            (A::Float64(a), A::Float64(b)) => A::new_float64(simd_op::<_, _, _, 32>(a.as_native(), b.as_native(), |a, b| a $op b).into_ordered()),

            #[cfg(not(feature = "simd"))]
            (A::Int32(a), A::Int32(b)) => A::new_int32(binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b)),
            #[cfg(not(feature = "simd"))]
            (A::Int64(a), A::Int64(b)) => A::new_int64(binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b)),
            #[cfg(not(feature = "simd"))]
            (A::Float64(a), A::Float64(b)) => A::new_float64(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op *b)),

            (A::Int32(a), A::Int64(b)) => A::new_int64(binary_op(a.as_ref(), b.as_ref(), |a, b| (*a as i64) $op *b)),
            (A::Int64(a), A::Int32(b)) => A::new_int64(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op (*b as i64))),

            (A::Int32(a), A::Float64(b)) => A::new_float64(binary_op(a.as_ref(), b.as_ref(), |a, b| F64::from(*a as f64) $op *b)),
            (A::Int64(a), A::Float64(b)) => A::new_float64(binary_op(a.as_ref(), b.as_ref(), |a, b| F64::from(*a as f64) $op *b)),
            (A::Float64(a), A::Int32(b)) => A::new_float64(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op F64::from(*b as f64))),
            (A::Float64(a), A::Int64(b)) => A::new_float64(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op F64::from(*b as f64))),

            (A::Int32(a), A::Decimal(b)) => A::new_decimal(binary_op(a.as_ref(), b.as_ref(), |a, b| Decimal::from(*a) $op *b)),
            (A::Int64(a), A::Decimal(b)) => A::new_decimal(binary_op(a.as_ref(), b.as_ref(), |a, b| Decimal::from(*a) $op *b)),
            (A::Float64(a), A::Decimal(b)) => A::new_decimal(binary_op(a.as_ref(), b.as_ref(), |a, b| Decimal::from_f64_retain(a.0).unwrap() $op *b)),
            (A::Decimal(a), A::Int32(b)) => A::new_decimal(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op Decimal::from(*b))),
            (A::Decimal(a), A::Int64(b)) => A::new_decimal(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op Decimal::from(*b))),
            (A::Decimal(a), A::Float64(b)) => A::new_decimal(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op Decimal::from_f64_retain(b.0).unwrap())),

            (A::Decimal(a), A::Decimal(b)) => A::new_decimal(binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b)),
            (A::Date(a), A::Interval(b)) => A::new_date(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op *b)),

            _ => return Err(ConvertError::NoBinaryOp(stringify!($name).into(), self.type_string(), other.type_string())),
        })
        }
    }
}

/// A macro to implement comparison operations.
macro_rules! cmp {
    ($name:ident, $op:tt) => {
        pub fn $name(
            &self,
            other: &Self,
        ) -> Result<Self, ConvertError> {
        Ok(match (self, other) {
            (A::Bool(a), A::Bool(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b)),
            (A::Int32(a), A::Int32(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b)),
            (A::Int64(a), A::Int64(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b)),
            (A::Float64(a), A::Float64(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op *b)),
            (A::Utf8(a), A::Utf8(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b)),
            (A::Date(a), A::Date(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b)),
            (A::Decimal(a), A::Decimal(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b)),

            (A::Int32(a), A::Int64(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| (*a as i64) $op *b)),
            (A::Int64(a), A::Int32(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op (*b as i64))),

            (A::Int32(a), A::Float64(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| F64::from(*a as f64) $op *b)),
            (A::Int64(a), A::Float64(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| F64::from(*a as f64) $op *b)),
            (A::Float64(a), A::Int32(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op F64::from(*b as f64))),
            (A::Float64(a), A::Int64(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op F64::from(*b as f64))),

            (A::Int32(a), A::Decimal(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| Decimal::from(*a) $op *b)),
            (A::Int64(a), A::Decimal(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| Decimal::from(*a) $op *b)),
            (A::Float64(a), A::Decimal(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| Decimal::from_f64_retain(a.0).unwrap() $op *b)),
            (A::Decimal(a), A::Int32(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op Decimal::from(*b))),
            (A::Decimal(a), A::Int64(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op Decimal::from(*b))),
            (A::Decimal(a), A::Float64(b)) => A::new_bool(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op Decimal::from_f64_retain(b.0).unwrap())),

            _ => return Err(ConvertError::NoBinaryOp(stringify!($name).into(), self.type_string(), other.type_string())),
        })
        }
    }
}

impl ArrayImpl {
    arith!(add, +);
    arith!(sub, -);
    arith!(mul, *);
    arith!(div, /);
    arith!(rem, %);
    cmp!(eq, ==);
    cmp!(ne, !=);
    cmp!(gt,  >);
    cmp!(lt,  <);
    cmp!(ge, >=);
    cmp!(le, <=);

    pub fn and(&self, other: &Self) -> Result<Self, ConvertError> {
        let (A::Bool(a), A::Bool(b)) = (self, other) else {
            return Err(ConvertError::NoBinaryOp("and".into(), self.type_string(), other.type_string()));
        };
        Ok(A::new_bool(binary_op_with_null(
            a.as_ref(),
            b.as_ref(),
            |a, b| match (a, b) {
                (Some(a), Some(b)) => Some(*a && *b),
                (Some(false), _) | (_, Some(false)) => Some(false),
                _ => None,
            },
        )))
    }

    pub fn or(&self, other: &Self) -> Result<Self, ConvertError> {
        let (A::Bool(a), A::Bool(b)) = (self, other) else {
            return Err(ConvertError::NoBinaryOp("or".into(), self.type_string(), other.type_string()));
        };
        Ok(A::new_bool(binary_op_with_null(
            a.as_ref(),
            b.as_ref(),
            |a, b| match (a, b) {
                (Some(a), Some(b)) => Some(*a || *b),
                (Some(true), _) | (_, Some(true)) => Some(true),
                _ => None,
            },
        )))
    }

    /// Perform binary operation.
    pub fn binary_op(
        &self,
        op: &BinaryOperator,
        other: &ArrayImpl,
    ) -> Result<ArrayImpl, ConvertError> {
        use BinaryOperator::*;
        match op {
            Plus => self.add(other),
            Minus => self.sub(other),
            Multiply => self.mul(other),
            Divide => self.div(other),
            Modulo => self.rem(other),
            Eq => self.eq(other),
            NotEq => self.ne(other),
            Gt => self.gt(other),
            Lt => self.lt(other),
            GtEq => self.ge(other),
            LtEq => self.le(other),
            And => self.and(other),
            Or => self.or(other),
            _ => Err(ConvertError::NoBinaryOp(
                op.to_string(),
                self.type_string(),
                other.type_string(),
            )),
        }
    }

    /// Cast the array to another type.
    pub fn cast(&self, data_type: &DataTypeKind) -> Result<Self, ConvertError> {
        type Type = DataTypeKind;
        Ok(match self {
            Self::Null(a) => {
                let mut builder =
                    ArrayBuilderImpl::with_capacity(a.len(), &data_type.clone().nullable());
                for _ in 0..a.len() {
                    builder.push(&DataValue::Null);
                }
                builder.finish()
            }
            Self::Bool(a) => match data_type {
                Type::Bool => Self::Bool(a.clone()),
                Type::Int32 => Self::new_int32(unary_op(a.as_ref(), |&b| b as i32)),
                Type::Int64 => Self::new_int64(unary_op(a.as_ref(), |&b| b as i64)),
                Type::Float64 => {
                    Self::new_float64(unary_op(a.as_ref(), |&b| F64::from(b as u8 as f64)))
                }
                Type::String => {
                    Self::new_utf8(unary_op(a.as_ref(), |&b| if b { "true" } else { "false" }))
                }
                Type::Decimal(_, _) => {
                    Self::new_decimal(unary_op(a.as_ref(), |&b| Decimal::from(b as u8)))
                }
                Type::Null | Type::Date | Type::Interval | Type::Blob | Type::Struct(_) => {
                    return Err(ConvertError::NoCast("BOOLEAN", data_type.clone()));
                }
            },
            Self::Int32(a) => match data_type {
                Type::Bool => Self::new_bool(unary_op(a.as_ref(), |&i| i != 0)),
                Type::Int32 => Self::Int32(a.clone()),
                Type::Int64 => Self::new_int64(unary_op(a.as_ref(), |&b| b as i64)),
                Type::Float64 => Self::new_float64(unary_op(a.as_ref(), |&i| F64::from(i as f64))),
                Type::String => Self::new_utf8(unary_op(a.as_ref(), |&i| i.to_string())),
                Type::Decimal(_, _) => {
                    Self::new_decimal(unary_op(a.as_ref(), |&i| Decimal::from(i)))
                }
                Type::Null | Type::Date | Type::Interval | Type::Blob | Type::Struct(_) => {
                    return Err(ConvertError::NoCast("INT", data_type.clone()));
                }
            },
            Self::Int64(a) => match data_type {
                Type::Bool => Self::new_bool(unary_op(a.as_ref(), |&i| i != 0)),
                Type::Int32 => Self::new_int32(try_unary_op(a.as_ref(), |&b| match b.to_i32() {
                    Some(d) => Ok(d),
                    None => Err(ConvertError::Overflow(DataValue::Int64(b), Type::Int32)),
                })?),
                Type::Int64 => Self::Int64(a.clone()),
                Type::Float64 => Self::new_float64(unary_op(a.as_ref(), |&i| F64::from(i as f64))),
                Type::String => Self::new_utf8(unary_op(a.as_ref(), |&i| i.to_string())),
                Type::Decimal(_, _) => {
                    Self::new_decimal(unary_op(a.as_ref(), |&i| Decimal::from(i)))
                }
                Type::Null | Type::Date | Type::Interval | Type::Blob | Type::Struct(_) => {
                    return Err(ConvertError::NoCast("BIGINT", data_type.clone()));
                }
            },
            Self::Float64(a) => match data_type {
                Type::Bool => Self::new_bool(unary_op(a.as_ref(), |&f| f != 0.0)),
                Type::Int32 => Self::new_int32(try_unary_op(a.as_ref(), |&b| match b.to_i32() {
                    Some(d) => Ok(d),
                    None => Err(ConvertError::Overflow(DataValue::Float64(b), Type::Int32)),
                })?),
                Type::Int64 => Self::new_int64(try_unary_op(a.as_ref(), |&b| match b.to_i64() {
                    Some(d) => Ok(d),
                    None => Err(ConvertError::Overflow(DataValue::Float64(b), Type::Int64)),
                })?),
                Type::Float64 => Self::Float64(a.clone()),
                Type::String => Self::new_utf8(unary_op(a.as_ref(), |&f| f.to_string())),
                Type::Decimal(_, scale) => {
                    Self::new_decimal(try_unary_op(
                        a.as_ref(),
                        |&f| match Decimal::from_f64_retain(f.0) {
                            Some(mut d) => {
                                if let Some(s) = scale {
                                    d.rescale(*s as u32);
                                }
                                Ok(d)
                            }
                            None => Err(ConvertError::ToDecimalError(DataValue::Float64(f))),
                        },
                    )?)
                }
                Type::Null | Type::Date | Type::Interval | Type::Blob | Type::Struct(_) => {
                    return Err(ConvertError::NoCast("DOUBLE", data_type.clone()));
                }
            },
            Self::Utf8(a) => match data_type {
                Type::Bool => Self::new_bool(try_unary_op(a.as_ref(), |s| {
                    s.parse::<bool>()
                        .map_err(|e| ConvertError::ParseBool(s.to_string(), e))
                })?),
                Type::Int32 => Self::new_int32(try_unary_op(a.as_ref(), |s| {
                    s.parse::<i32>()
                        .map_err(|e| ConvertError::ParseInt(s.to_string(), e))
                })?),
                Type::Int64 => Self::new_int64(try_unary_op(a.as_ref(), |s| {
                    s.parse::<i64>()
                        .map_err(|e| ConvertError::ParseInt(s.to_string(), e))
                })?),
                Type::Float64 => Self::new_float64(try_unary_op(a.as_ref(), |s| {
                    s.parse::<F64>()
                        .map_err(|e| ConvertError::ParseFloat(s.to_string(), e))
                })?),
                Type::String => Self::Utf8(a.clone()),
                Type::Decimal(_, _) => Self::new_decimal(try_unary_op(a.as_ref(), |s| {
                    Decimal::from_str(s).map_err(|e| ConvertError::ParseDecimal(s.to_string(), e))
                })?),
                Type::Date => Self::new_date(try_unary_op(a.as_ref(), |s| {
                    Date::from_str(s).map_err(|e| ConvertError::ParseDate(s.to_string(), e))
                })?),
                Type::Interval => Self::new_interval(try_unary_op(a.as_ref(), |s| {
                    Interval::from_str(s).map_err(|e| ConvertError::ParseInterval(s.to_string(), e))
                })?),
                Type::Blob => Self::new_blob(try_unary_op(a.as_ref(), |s| {
                    Blob::from_str(s).map_err(|e| ConvertError::ParseBlob(s.to_string(), e))
                })?),
                Type::Null | Type::Struct(_) => {
                    return Err(ConvertError::NoCast("VARCHAR", data_type.clone()));
                }
            },
            Self::Blob(_) => todo!("cast array"),
            Self::Decimal(a) => match data_type {
                Type::Bool => Self::new_bool(unary_op(a.as_ref(), |&d| !d.is_zero())),
                Type::Int32 => Self::new_int32(try_unary_op(a.as_ref(), |&d| {
                    d.to_i32().ok_or(ConvertError::FromDecimalError(
                        DataTypeKind::Int32,
                        DataValue::Decimal(d),
                    ))
                })?),
                Type::Int64 => Self::new_int64(try_unary_op(a.as_ref(), |&d| {
                    d.to_i64().ok_or(ConvertError::FromDecimalError(
                        DataTypeKind::Int64,
                        DataValue::Decimal(d),
                    ))
                })?),
                Type::Float64 => Self::new_float64(try_unary_op(a.as_ref(), |&d| {
                    d.to_f64()
                        .map(F64::from)
                        .ok_or(ConvertError::FromDecimalError(
                            DataTypeKind::Float64,
                            DataValue::Decimal(d),
                        ))
                })?),
                Type::String => Self::new_utf8(unary_op(a.as_ref(), |d| d.to_string())),
                Type::Decimal(_, _) => self.clone(),
                Type::Null | Type::Blob | Type::Date | Type::Interval | Type::Struct(_) => {
                    return Err(ConvertError::NoCast("DOUBLE", data_type.clone()));
                }
            },
            Self::Date(a) => match data_type {
                Type::Date => self.clone(),
                Type::String => Self::new_utf8(unary_op(a.as_ref(), |&d| d.to_string())),
                _ => return Err(ConvertError::NoCast("DATE", data_type.clone())),
            },
            Self::Interval(a) => match data_type {
                Type::Interval => self.clone(),
                Type::String => Self::new_utf8(unary_op(a.as_ref(), |&d| d.to_string())),
                _ => return Err(ConvertError::NoCast("INTERVAL", data_type.clone())),
            },
        })
    }

    /// Returns the sum of values.
    pub fn sum(&self) -> DataValue {
        match self {
            Self::Int32(a) => DataValue::Int32(a.iter().flatten().sum()),
            Self::Int64(a) => DataValue::Int64(a.iter().flatten().sum()),
            Self::Float64(a) => DataValue::Float64(a.iter().flatten().sum()),
            Self::Decimal(a) => DataValue::Decimal(a.iter().flatten().sum()),
            Self::Interval(a) => DataValue::Interval(a.iter().flatten().sum()),
            _ => panic!("can not sum array"),
        }
    }
}

/// Implement aggregation functions.
macro_rules! impl_agg {
    ([], $( { $Abc:ident, $Type:ty, $abc:ident, $AbcArray:ty, $AbcArrayBuilder:ty, $Value:ident, $Pattern:pat } ),*) => {
        impl ArrayImpl {
            /// Returns the minimum of values.
            pub fn min_(&self) -> DataValue {
                match self {
                    $(Self::$Abc(a) => a.iter().flatten().min().into(),)*
                }
            }

            /// Returns the maximum of values.
            pub fn max_(&self) -> DataValue {
                match self {
                    $(Self::$Abc(a) => a.iter().flatten().max().into(),)*
                }
            }

            /// Returns the first non-null value.
            pub fn first(&self) -> DataValue {
                match self {
                    $(Self::$Abc(a) => a.iter().next().flatten().into(),)*
                }
            }

            /// Returns the last non-null value.
            pub fn last(&self) -> DataValue {
                match self {
                    $(Self::$Abc(a) => a.iter().rev().next().flatten().into(),)*
                }
            }
        }
    }
}

for_all_variants! { impl_agg }

pub fn simd_op<T, O, F, const N: usize>(
    a: &PrimitiveArray<T>,
    b: &PrimitiveArray<T>,
    f: F,
) -> PrimitiveArray<O>
where
    T: NativeType + SimdElement,
    O: NativeType + SimdElement,
    F: Fn(Simd<T, N>, Simd<T, N>) -> Simd<O, N>,
    LaneCount<N>: SupportedLaneCount,
{
    assert_eq!(a.len(), b.len());
    a.batch_iter::<N>()
        .zip(b.batch_iter::<N>())
        .map(|(a, b)| BatchItem {
            valid: a.valid & b.valid,
            data: f(a.data, b.data),
            len: a.len,
        })
        .collect()
}

pub fn binary_op<A, B, O, F, V>(a: &A, b: &B, f: F) -> O
where
    A: Array,
    B: Array,
    O: Array,
    V: Borrow<O::Item>,
    F: Fn(&A::Item, &B::Item) -> V,
{
    assert_eq!(a.len(), b.len());
    let mut builder = O::Builder::with_capacity(a.len());
    for (a, b) in a.iter().zip(b.iter()) {
        if let (Some(a), Some(b)) = (a, b) {
            builder.push(Some(f(a, b).borrow()));
        } else {
            builder.push(None);
        }
    }
    builder.finish()
}

pub fn binary_op_masks<A, B, O, F>(a: &A, b: &B, f: F) -> O
where
    A: ArrayValidExt,
    B: ArrayValidExt,
    O: ArrayFromDataExt,
    O::Item: Sized,
    F: Fn(&A::Item, &B::Item) -> <O::Item as ToOwned>::Owned,
{
    assert_eq!(a.len(), b.len());
    let it = a.raw_iter().zip(b.raw_iter()).map(|(a, b)| f(a, b));
    let valid = a.get_valid_bitmap().clone() & b.get_valid_bitmap().clone();
    O::from_data(it, valid)
}

fn binary_op_with_null<A, B, O, F, V>(a: &A, b: &B, f: F) -> O
where
    A: Array,
    B: Array,
    O: Array,
    V: Borrow<O::Item>,
    F: Fn(Option<&A::Item>, Option<&B::Item>) -> Option<V>,
{
    assert_eq!(a.len(), b.len());
    let mut builder = O::Builder::with_capacity(a.len());
    for (a, b) in a.iter().zip(b.iter()) {
        if let Some(c) = f(a, b) {
            builder.push(Some(c.borrow()));
        } else {
            builder.push(None);
        }
    }
    builder.finish()
}

fn unary_op<A, O, F, V>(a: &A, f: F) -> O
where
    A: Array,
    O: Array,
    V: Borrow<O::Item>,
    F: Fn(&A::Item) -> V,
{
    let mut builder = O::Builder::with_capacity(a.len());
    for e in a.iter() {
        if let Some(e) = e {
            builder.push(Some(f(e).borrow()));
        } else {
            builder.push(None);
        }
    }
    builder.finish()
}

fn try_unary_op<A, O, F, V, E>(a: &A, f: F) -> Result<O, E>
where
    A: Array,
    O: Array,
    V: Borrow<O::Item>,
    F: Fn(&A::Item) -> Result<V, E>,
{
    let mut builder = O::Builder::with_capacity(a.len());
    for e in a.iter() {
        if let Some(e) = e {
            builder.push(Some(f(e)?.borrow()));
        } else {
            builder.push(None);
        }
    }
    Ok(builder.finish())
}
