// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! Array operations.

use std::borrow::Borrow;

use num_traits::ToPrimitive;
use regex::Regex;
use rust_decimal::prelude::FromStr;
use rust_decimal::Decimal;

use super::*;
use crate::for_all_variants;
use crate::parser::{BinaryOperator, UnaryOperator};
use crate::types::{
    Blob, ConvertError, DataType, DataValue, Date, DateTimeField, Interval, NativeType, Timestamp,
    TimestampTz, F64,
};

type A = ArrayImpl;
type Result = std::result::Result<ArrayImpl, ConvertError>;

impl ArrayImpl {
    pub fn neg(&self) -> Result {
        Ok(match self {
            A::Int32(a) => A::new_int32(unary_op(a.as_ref(), |v| -v)),
            A::Int64(a) => A::new_int64(unary_op(a.as_ref(), |v| -v)),
            A::Float64(a) => A::new_float64(unary_op(a.as_ref(), |v| -v)),
            A::Decimal(a) => A::new_decimal(unary_op(a.as_ref(), |v| -v)),
            _ => return Err(ConvertError::NoUnaryOp("-".into(), self.type_string())),
        })
    }

    /// Perform unary operation.
    pub fn unary_op(&self, op: &UnaryOperator) -> Result {
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
        ) -> Result {
        Ok(match (self, other) {
            (A::Int16(a), A::Int16(b)) => A::new_int16(binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b)),

            (A::Int16(a), A::Int32(b)) => A::new_int32(binary_op(a.as_ref(), b.as_ref(), |a, b| (*a as i32) $op *b)),
            (A::Int32(a), A::Int16(b)) => A::new_int32(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op (*b as i32))),
            (A::Int32(a), A::Int32(b)) => A::new_int32(binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b)),

            (A::Int16(a), A::Int64(b)) => A::new_int64(binary_op(a.as_ref(), b.as_ref(), |a, b| (*a as i64) $op *b)),
            (A::Int32(a), A::Int64(b)) => A::new_int64(binary_op(a.as_ref(), b.as_ref(), |a, b| (*a as i64) $op *b)),
            (A::Int64(a), A::Int16(b)) => A::new_int64(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op (*b as i64))),
            (A::Int64(a), A::Int32(b)) => A::new_int64(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op (*b as i64))),
            (A::Int64(a), A::Int64(b)) => A::new_int64(binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b)),

            (A::Int16(a), A::Float64(b)) => A::new_float64(binary_op(a.as_ref(), b.as_ref(), |a, b| F64::from(*a as f64) $op *b)),
            (A::Int32(a), A::Float64(b)) => A::new_float64(binary_op(a.as_ref(), b.as_ref(), |a, b| F64::from(*a as f64) $op *b)),
            (A::Int64(a), A::Float64(b)) => A::new_float64(binary_op(a.as_ref(), b.as_ref(), |a, b| F64::from(*a as f64) $op *b)),
            (A::Float64(a), A::Int16(b)) => A::new_float64(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op F64::from(*b as f64))),
            (A::Float64(a), A::Int32(b)) => A::new_float64(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op F64::from(*b as f64))),
            (A::Float64(a), A::Int64(b)) => A::new_float64(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op F64::from(*b as f64))),
            (A::Float64(a), A::Float64(b)) => A::new_float64(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op *b)),

            (A::Int16(a), A::Decimal(b)) => A::new_decimal(binary_op(a.as_ref(), b.as_ref(), |a, b| Decimal::from(*a) $op *b)),
            (A::Int32(a), A::Decimal(b)) => A::new_decimal(binary_op(a.as_ref(), b.as_ref(), |a, b| Decimal::from(*a) $op *b)),
            (A::Int64(a), A::Decimal(b)) => A::new_decimal(binary_op(a.as_ref(), b.as_ref(), |a, b| Decimal::from(*a) $op *b)),
            (A::Float64(a), A::Decimal(b)) => A::new_decimal(binary_op(a.as_ref(), b.as_ref(), |a, b| Decimal::from_f64_retain(a.0).unwrap() $op *b)),
            (A::Decimal(a), A::Int16(b)) => A::new_decimal(binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op Decimal::from(*b))),
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
        ) -> Result {
        Ok(A::new_bool(clear_null(match (self, other) {
            (A::Bool(a), A::Bool(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b),

            (A::Int16(a), A::Int16(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b),

            (A::Int16(a), A::Int32(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| (*a as i32) $op *b),
            (A::Int32(a), A::Int16(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op (*b as i32)),
            (A::Int32(a), A::Int32(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b),

            (A::Int16(a), A::Int64(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| (*a as i64) $op *b),
            (A::Int32(a), A::Int64(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| (*a as i64) $op *b),
            (A::Int64(a), A::Int16(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op (*b as i64)),
            (A::Int64(a), A::Int32(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op (*b as i64)),
            (A::Int64(a), A::Int64(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b),

            (A::Int16(a), A::Float64(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| F64::from(*a as f64) $op *b),
            (A::Int32(a), A::Float64(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| F64::from(*a as f64) $op *b),
            (A::Int64(a), A::Float64(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| F64::from(*a as f64) $op *b),
            (A::Float64(a), A::Int16(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op F64::from(*b as f64)),
            (A::Float64(a), A::Int32(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op F64::from(*b as f64)),
            (A::Float64(a), A::Int64(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op F64::from(*b as f64)),
            (A::Float64(a), A::Float64(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op *b),

            (A::Int16(a), A::Decimal(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| Decimal::from(*a) $op *b),
            (A::Int32(a), A::Decimal(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| Decimal::from(*a) $op *b),
            (A::Int64(a), A::Decimal(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| Decimal::from(*a) $op *b),
            (A::Float64(a), A::Decimal(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| Decimal::from_f64_retain(a.0).unwrap() $op *b),
            (A::Decimal(a), A::Int16(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op Decimal::from(*b)),
            (A::Decimal(a), A::Int32(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op Decimal::from(*b)),
            (A::Decimal(a), A::Int64(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op Decimal::from(*b)),
            (A::Decimal(a), A::Float64(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| *a $op Decimal::from_f64_retain(b.0).unwrap()),
            (A::Decimal(a), A::Decimal(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b),

            (A::String(a), A::String(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b),

            (A::Date(a), A::Date(b)) => binary_op(a.as_ref(), b.as_ref(), |a, b| a $op b),

            _ => return Err(ConvertError::NoBinaryOp(stringify!($name).into(), self.type_string(), other.type_string())),
        })))
        }
    }
}

impl ArrayImpl {
    arith!(add, +);
    arith!(sub, -);
    arith!(mul, *);
    arith!(unchecked_div, /);
    arith!(rem, %);
    cmp!(eq, ==);
    cmp!(ne, !=);
    cmp!(gt,  >);
    cmp!(lt,  <);
    cmp!(ge, >=);
    cmp!(le, <=);

    pub fn div(&self, other: &Self) -> Result {
        let valid_rhs = other.get_valid_bitmap();
        let other = safen_dividend(other, valid_rhs).ok_or(ConvertError::NoBinaryOp(
            "div".into(),
            self.type_string(),
            other.type_string(),
        ))?;

        self.unchecked_div(&other)
    }

    pub fn and(&self, other: &Self) -> Result {
        let (A::Bool(a), A::Bool(b)) = (self, other) else {
            return Err(ConvertError::NoBinaryOp(
                "and".into(),
                self.type_string(),
                other.type_string(),
            ));
        };
        let mut c: BoolArray = binary_op(a.as_ref(), b.as_ref(), |a, b| *a && *b);
        let a_false = a.to_raw_bitvec().not_then_and(a.get_valid_bitmap());
        let b_false = b.to_raw_bitvec().not_then_and(b.get_valid_bitmap());
        c.get_valid_bitmap_mut().or(&a_false);
        c.get_valid_bitmap_mut().or(&b_false);
        Ok(A::new_bool(c))
    }

    pub fn or(&self, other: &Self) -> Result {
        let (A::Bool(a), A::Bool(b)) = (self, other) else {
            return Err(ConvertError::NoBinaryOp(
                "or".into(),
                self.type_string(),
                other.type_string(),
            ));
        };
        let mut c: BoolArray = binary_op(a.as_ref(), b.as_ref(), |a, b| *a || *b);
        let bitmap = c.to_raw_bitvec();
        c.get_valid_bitmap_mut().or(&bitmap);
        Ok(A::new_bool(c))
    }

    pub fn not(&self) -> Result {
        let A::Bool(a) = self else {
            return Err(ConvertError::NoUnaryOp("not".into(), self.type_string()));
        };
        Ok(A::new_bool(clear_null(unary_op(a.as_ref(), |b| !b))))
    }

    pub fn like(&self, pattern: &str) -> Result {
        /// Converts a SQL LIKE pattern to a regex pattern.
        fn like_to_regex(pattern: &str) -> String {
            let mut regex = String::with_capacity(pattern.len());
            regex.push('^');
            for c in pattern.chars() {
                match c {
                    '%' => regex.push_str(".*"),
                    '_' => regex.push('.'),
                    c => regex.push(c),
                }
            }
            regex.push('$');
            regex
        }
        let A::String(a) = self else {
            return Err(ConvertError::NoUnaryOp("like".into(), self.type_string()));
        };
        let regex = Regex::new(&like_to_regex(pattern)).unwrap();
        Ok(A::new_bool(clear_null(unary_op(a.as_ref(), |s| {
            regex.is_match(s)
        }))))
    }

    pub fn concat(&self, other: &Self) -> Result {
        let (A::String(a), A::String(b)) = (self, other) else {
            return Err(ConvertError::NoBinaryOp(
                "||".into(),
                self.type_string(),
                other.type_string(),
            ));
        };

        Ok(A::new_string(binary_op(a.as_ref(), b.as_ref(), |a, b| {
            format!("{a}{b}")
        })))
    }

    pub fn extract(&self, field: &DateTimeField) -> Result {
        Ok(match self {
            A::Date(a) => match &field.0 {
                sqlparser::ast::DateTimeField::Year => {
                    A::new_int32(unary_op(a.as_ref(), |d| d.year()))
                }
                sqlparser::ast::DateTimeField::Month => {
                    A::new_int32(unary_op(a.as_ref(), |d| d.month()))
                }
                sqlparser::ast::DateTimeField::Day => {
                    A::new_int32(unary_op(a.as_ref(), |d| d.day()))
                }
                f => todo!("extract {f} from date"),
            },
            A::Interval(_) => todo!("extract {field} from interval"),
            _ => {
                return Err(ConvertError::NoUnaryOp(
                    "extract".into(),
                    self.type_string(),
                ));
            }
        })
    }

    /// Select values from `true_array` or `false_array` according to the boolean value of `self`.
    pub fn select(&self, true_array: &Self, false_array: &Self) -> Result {
        let A::Bool(s) = self else {
            return Err(ConvertError::NoUnaryOp("case".into(), self.type_string()));
        };
        Ok(match (true_array, false_array) {
            (A::Int16(a), A::Int16(b)) => {
                A::new_int16(select_op(s.as_ref(), a.as_ref(), b.as_ref()))
            }
            (A::Int32(a), A::Int32(b)) => {
                A::new_int32(select_op(s.as_ref(), a.as_ref(), b.as_ref()))
            }
            (A::Int64(a), A::Int64(b)) => {
                A::new_int64(select_op(s.as_ref(), a.as_ref(), b.as_ref()))
            }
            (A::Float64(a), A::Float64(b)) => {
                A::new_float64(select_op(s.as_ref(), a.as_ref(), b.as_ref()))
            }
            (A::Decimal(a), A::Decimal(b)) => {
                A::new_decimal(select_op(s.as_ref(), a.as_ref(), b.as_ref()))
            }
            (A::Date(a), A::Date(b)) => A::new_date(select_op(s.as_ref(), a.as_ref(), b.as_ref())),
            (A::Interval(a), A::Interval(b)) => {
                A::new_interval(select_op(s.as_ref(), a.as_ref(), b.as_ref()))
            }
            _ => {
                return Err(ConvertError::NoBinaryOp(
                    "case".into(),
                    true_array.type_string(),
                    false_array.type_string(),
                ))
            }
        })
    }

    pub fn substring(&self, start: &Self, length: &Self) -> Result {
        let (A::String(a), A::Int32(b), A::Int32(c)) = (self, start, length) else {
            return Err(ConvertError::NoTernaryOp(
                "substring".into(),
                self.type_string(),
                start.type_string(),
                length.type_string(),
            ));
        };
        Ok(A::new_string(ternary_op(
            a.as_ref(),
            b.as_ref(),
            c.as_ref(),
            |a, b, c| {
                let chars = a.chars().count() as i32;
                let mut start = match *b {
                    0.. => *b - 1,
                    _ => chars + *b,
                };
                let mut end = start.saturating_add(*c);
                if start > end {
                    (start, end) = (end, start);
                }
                let skip = start.max(0);
                let take = (end - skip).max(0);
                a.chars()
                    .skip(skip as usize)
                    .take(take as usize)
                    .collect::<String>()
            },
        )))
    }

    /// Perform binary operation.
    pub fn binary_op(&self, op: &BinaryOperator, other: &ArrayImpl) -> Result {
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
            StringConcat => self.concat(other),
            _ => Err(ConvertError::NoBinaryOp(
                op.to_string(),
                self.type_string(),
                other.type_string(),
            )),
        }
    }

    /// Cast the array to another type.
    pub fn cast(&self, data_type: &DataType) -> Result {
        type Type = DataType;
        Ok(match self {
            Self::Null(a) => {
                let mut builder = ArrayBuilderImpl::with_capacity(a.len(), data_type);
                builder.push_n(a.len(), &DataValue::Null);
                builder.finish()
            }
            Self::Bool(a) => match data_type {
                Type::Bool => Self::Bool(a.clone()),
                Type::Int16 => Self::new_int16(unary_op(a.as_ref(), |&b| b as i16)),
                Type::Int32 => Self::new_int32(unary_op(a.as_ref(), |&b| b as i32)),
                Type::Int64 => Self::new_int64(unary_op(a.as_ref(), |&b| b as i64)),
                Type::Float64 => {
                    Self::new_float64(unary_op(a.as_ref(), |&b| F64::from(b as u8 as f64)))
                }
                Type::String => {
                    Self::new_string(unary_op(a.as_ref(), |&b| if b { "true" } else { "false" }))
                }
                Type::Decimal(_, _) => {
                    Self::new_decimal(unary_op(a.as_ref(), |&b| Decimal::from(b as u8)))
                }
                Type::Null
                | Type::Date
                | Type::Timestamp
                | Type::TimestampTz
                | Type::Interval
                | Type::Blob
                | Type::Vector(_)
                | Type::Struct(_) => {
                    return Err(ConvertError::NoCast("BOOLEAN", data_type.clone()));
                }
            },
            Self::Int16(a) => match data_type {
                Type::Bool => Self::new_bool(unary_op(a.as_ref(), |&i| i != 0)),
                Type::Int16 => Self::Int16(a.clone()),
                Type::Int32 => Self::new_int32(unary_op(a.as_ref(), |&b| b as i32)),
                Type::Int64 => Self::new_int64(unary_op(a.as_ref(), |&b| b as i64)),
                Type::Float64 => Self::new_float64(unary_op(a.as_ref(), |&i| F64::from(i as f64))),
                Type::String => Self::new_string(StringArray::from_iter_display(a.iter())),
                Type::Decimal(_, _) => {
                    Self::new_decimal(unary_op(a.as_ref(), |&i| Decimal::from(i)))
                }
                Type::Null
                | Type::Date
                | Type::Timestamp
                | Type::TimestampTz
                | Type::Interval
                | Type::Blob
                | Type::Vector(_)
                | Type::Struct(_) => {
                    return Err(ConvertError::NoCast("SMALLINT", data_type.clone()));
                }
            },
            Self::Int32(a) => match data_type {
                Type::Bool => Self::new_bool(unary_op(a.as_ref(), |&i| i != 0)),
                Type::Int16 => Self::new_int16(try_unary_op(a.as_ref(), |&b| {
                    b.to_i16()
                        .ok_or(ConvertError::Overflow(DataValue::Int32(b), Type::Int16))
                })?),
                Type::Int32 => Self::Int32(a.clone()),
                Type::Int64 => Self::new_int64(unary_op(a.as_ref(), |&b| b as i64)),
                Type::Float64 => Self::new_float64(unary_op(a.as_ref(), |&i| F64::from(i as f64))),
                Type::String => Self::new_string(StringArray::from_iter_display(a.iter())),
                Type::Decimal(_, _) => {
                    Self::new_decimal(unary_op(a.as_ref(), |&i| Decimal::from(i)))
                }
                Type::Null
                | Type::Date
                | Type::Timestamp
                | Type::TimestampTz
                | Type::Interval
                | Type::Blob
                | Type::Vector(_)
                | Type::Struct(_) => {
                    return Err(ConvertError::NoCast("INT", data_type.clone()));
                }
            },
            Self::Int64(a) => match data_type {
                Type::Bool => Self::new_bool(unary_op(a.as_ref(), |&i| i != 0)),
                Type::Int16 => Self::new_int16(try_unary_op(a.as_ref(), |&b| {
                    b.to_i16()
                        .ok_or(ConvertError::Overflow(DataValue::Int64(b), Type::Int16))
                })?),
                Type::Int32 => Self::new_int32(try_unary_op(a.as_ref(), |&b| {
                    b.to_i32()
                        .ok_or(ConvertError::Overflow(DataValue::Int64(b), Type::Int32))
                })?),
                Type::Int64 => Self::Int64(a.clone()),
                Type::Float64 => Self::new_float64(unary_op(a.as_ref(), |&i| F64::from(i as f64))),
                Type::String => Self::new_string(StringArray::from_iter_display(a.iter())),
                Type::Decimal(_, _) => {
                    Self::new_decimal(unary_op(a.as_ref(), |&i| Decimal::from(i)))
                }
                Type::Null
                | Type::Date
                | Type::Timestamp
                | Type::TimestampTz
                | Type::Interval
                | Type::Blob
                | Type::Vector(_)
                | Type::Struct(_) => {
                    return Err(ConvertError::NoCast("BIGINT", data_type.clone()));
                }
            },
            Self::Float64(a) => match data_type {
                Type::Bool => Self::new_bool(unary_op(a.as_ref(), |&f| f != 0.0)),
                Type::Int16 => Self::new_int16(try_unary_op(a.as_ref(), |&b| {
                    b.to_i16()
                        .ok_or(ConvertError::Overflow(DataValue::Float64(b), Type::Int16))
                })?),
                Type::Int32 => Self::new_int32(try_unary_op(a.as_ref(), |&b| {
                    b.to_i32()
                        .ok_or(ConvertError::Overflow(DataValue::Float64(b), Type::Int32))
                })?),
                Type::Int64 => Self::new_int64(try_unary_op(a.as_ref(), |&b| {
                    b.to_i64()
                        .ok_or(ConvertError::Overflow(DataValue::Float64(b), Type::Int64))
                })?),
                Type::Float64 => Self::Float64(a.clone()),
                Type::String => Self::new_string(StringArray::from_iter_display(a.iter())),
                Type::Decimal(_, _) => Self::new_decimal(unary_op(a.as_ref(), |&f| {
                    Decimal::from_f64_retain(f.0).unwrap()
                })),
                Type::Null
                | Type::Date
                | Type::Timestamp
                | Type::TimestampTz
                | Type::Interval
                | Type::Blob
                | Type::Vector(_)
                | Type::Struct(_) => {
                    return Err(ConvertError::NoCast("DOUBLE", data_type.clone()));
                }
            },
            Self::String(a) => match data_type {
                Type::Bool => Self::new_bool(try_unary_op(a.as_ref(), |s| {
                    s.parse::<bool>()
                        .map_err(|e| ConvertError::ParseBool(s.to_string(), e))
                })?),
                Type::Int16 => Self::new_int16(try_unary_op(a.as_ref(), |s| {
                    s.parse::<i16>()
                        .map_err(|e| ConvertError::ParseInt(s.to_string(), e))
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
                Type::String => Self::String(a.clone()),
                Type::Decimal(_, _) => Self::new_decimal(try_unary_op(a.as_ref(), |s| {
                    Decimal::from_str(s).map_err(|e| ConvertError::ParseDecimal(s.to_string(), e))
                })?),
                Type::Date => Self::new_date(try_unary_op(a.as_ref(), |s| {
                    Date::from_str(s).map_err(|e| ConvertError::ParseDate(s.to_string(), e))
                })?),
                Type::Timestamp => Self::new_timestamp(try_unary_op(a.as_ref(), |s| {
                    Timestamp::from_str(s)
                        .map_err(|e| ConvertError::ParseTimestamp(s.to_string(), e))
                })?),
                Type::TimestampTz => Self::new_timestamp_tz(try_unary_op(a.as_ref(), |s| {
                    TimestampTz::from_str(s)
                        .map_err(|e| ConvertError::ParseTimestampTz(s.to_string(), e))
                })?),
                Type::Interval => Self::new_interval(try_unary_op(a.as_ref(), |s| {
                    Interval::from_str(s).map_err(|e| ConvertError::ParseInterval(s.to_string(), e))
                })?),
                Type::Blob => Self::new_blob(try_unary_op(a.as_ref(), |s| {
                    Blob::from_str(s).map_err(|e| ConvertError::ParseBlob(s.to_string(), e))
                })?),
                Type::Vector(_) => Self::new_vector(try_unary_op(a.as_ref(), |s| {
                    Vector::from_str(s).map_err(|e| ConvertError::ParseVector(s.to_string(), e))
                })?),
                Type::Null | Type::Struct(_) => {
                    return Err(ConvertError::NoCast("VARCHAR", data_type.clone()));
                }
            },
            Self::Blob(_) => todo!("cast array"),
            Self::Vector(_) => todo!("cast array"),
            Self::Decimal(a) => match data_type {
                Type::Bool => Self::new_bool(unary_op(a.as_ref(), |&d| !d.is_zero())),
                Type::Int16 => Self::new_int16(try_unary_op(a.as_ref(), |&d| {
                    d.to_i16()
                        .ok_or(ConvertError::FromDecimalError(DataType::Int16, d))
                })?),
                Type::Int32 => Self::new_int32(try_unary_op(a.as_ref(), |&d| {
                    d.to_i32()
                        .ok_or(ConvertError::FromDecimalError(DataType::Int32, d))
                })?),
                Type::Int64 => Self::new_int64(try_unary_op(a.as_ref(), |&d| {
                    d.to_i64()
                        .ok_or(ConvertError::FromDecimalError(DataType::Int64, d))
                })?),
                Type::Float64 => Self::new_float64(try_unary_op(a.as_ref(), |&d| {
                    d.to_f64()
                        .map(F64::from)
                        .ok_or(ConvertError::FromDecimalError(DataType::Float64, d))
                })?),
                Type::String => Self::new_string(StringArray::from_iter_display(a.iter())),
                Type::Decimal(_, _) => self.clone(),
                Type::Null
                | Type::Blob
                | Type::Date
                | Type::Timestamp
                | Type::TimestampTz
                | Type::Interval
                | Type::Vector(_)
                | Type::Struct(_) => {
                    return Err(ConvertError::NoCast("DOUBLE", data_type.clone()));
                }
            },
            Self::Date(a) => match data_type {
                Type::Date => self.clone(),
                Type::String => Self::new_string(StringArray::from_iter_display(a.iter())),
                _ => return Err(ConvertError::NoCast("DATE", data_type.clone())),
            },
            Self::Timestamp(a) => match data_type {
                Type::Timestamp => self.clone(),
                Type::String => Self::new_string(StringArray::from_iter_display(a.iter())),
                _ => return Err(ConvertError::NoCast("TIMESTAMP", data_type.clone())),
            },
            Self::TimestampTz(a) => match data_type {
                Type::TimestampTz => self.clone(),
                Type::String => Self::new_string(StringArray::from_iter_display(a.iter())),
                _ => {
                    return Err(ConvertError::NoCast(
                        "TIMESTAMP WITH TIME ZONE",
                        data_type.clone(),
                    ))
                }
            },
            Self::Interval(a) => match data_type {
                Type::Interval => self.clone(),
                Type::String => Self::new_string(StringArray::from_iter_display(a.iter())),
                _ => return Err(ConvertError::NoCast("INTERVAL", data_type.clone())),
            },
        })
    }

    /// Returns the sum of values.
    pub fn sum(&self) -> DataValue {
        match self {
            Self::Int16(a) => DataValue::Int16(a.raw_iter().sum()),
            Self::Int32(a) => DataValue::Int32(a.raw_iter().sum()),
            Self::Int64(a) => DataValue::Int64(a.raw_iter().sum()),
            Self::Float64(a) => DataValue::Float64(a.raw_iter().sum()),
            Self::Decimal(a) => DataValue::Decimal(a.raw_iter().sum()),
            Self::Interval(a) => DataValue::Interval(a.raw_iter().sum()),
            _ => panic!("can not sum array"),
        }
    }

    /// Returns the number of non-null values.
    pub fn count(&self) -> usize {
        self.get_valid_bitmap().count_ones()
    }

    pub fn replace(&self, from: &str, to: &str) -> Result {
        let A::String(a) = self else {
            return Err(ConvertError::NoUnaryOp(
                "replace".into(),
                self.type_string(),
            ));
        };
        Ok(A::new_string(unary_op(a.as_ref(), |s| s.replace(from, to))))
    }

    pub fn vector_l2_distance(&self, other: &ArrayImpl) -> Result {
        let ArrayImpl::Vector(a) = self else {
            return Err(ConvertError::NoBinaryOp(
                "vector_l2_distance".into(),
                self.type_string(),
                other.type_string(),
            ));
        };
        let ArrayImpl::Vector(b) = other else {
            return Err(ConvertError::NoBinaryOp(
                "vector_l2_distance".into(),
                other.type_string(),
                self.type_string(),
            ));
        };
        Ok(ArrayImpl::new_float64(binary_op(
            a.as_ref(),
            b.as_ref(),
            |a, b| a.l2_distance(b),
        )))
    }

    pub fn vector_cosine_distance(&self, other: &ArrayImpl) -> Result {
        let ArrayImpl::Vector(a) = self else {
            return Err(ConvertError::NoBinaryOp(
                "vector_cosine_distance".into(),
                self.type_string(),
                other.type_string(),
            ));
        };
        let ArrayImpl::Vector(b) = other else {
            return Err(ConvertError::NoBinaryOp(
                "vector_cosine_distance".into(),
                other.type_string(),
                self.type_string(),
            ));
        };
        Ok(ArrayImpl::new_float64(binary_op(
            a.as_ref(),
            b.as_ref(),
            |a, b| a.cosine_distance(b),
        )))
    }

    pub fn vector_neg_inner_product(&self, other: &ArrayImpl) -> Result {
        let ArrayImpl::Vector(a) = self else {
            return Err(ConvertError::NoBinaryOp(
                "vector_neg_inner_product".into(),
                self.type_string(),
                other.type_string(),
            ));
        };
        let ArrayImpl::Vector(b) = other else {
            return Err(ConvertError::NoBinaryOp(
                "vector_neg_inner_product".into(),
                other.type_string(),
                self.type_string(),
            ));
        };
        Ok(ArrayImpl::new_float64(binary_op(
            a.as_ref(),
            b.as_ref(),
            |a, b| -a.dot_product(b),
        )))
    }
}

/// Implement aggregation functions.
macro_rules! impl_agg {
    ([], $( { $Abc:ident, $Type:ty, $abc:ident, $AbcArray:ty, $AbcArrayBuilder:ty, $Value:ident, $Pattern:pat } ),*) => {
        impl ArrayImpl {
            /// Returns the minimum of values.
            pub fn min_(&self) -> DataValue {
                match self {
                    $(Self::$Abc(a) => a.nonnull_iter().min().into(),)*
                }
            }

            /// Returns the maximum of values.
            pub fn max_(&self) -> DataValue {
                match self {
                    $(Self::$Abc(a) => a.nonnull_iter().max().into(),)*
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

fn safen_dividend(array: &ArrayImpl, valid: &BitVec) -> Option<ArrayImpl> {
    fn f<T, N>(array: &PrimitiveArray<N>, valid: &BitVec, value: N) -> T
    where
        T: ArrayFromDataExt,
        N: NativeType + num_traits::Zero + Borrow<<T as Array>::Item>,
    {
        let mut valid = valid.to_owned();

        // 1. set valid as false if item is zero
        for (idx, item) in array.raw_iter().enumerate() {
            if item.is_zero() {
                valid.set(idx, false);
            }
        }

        // 2. replace item with safe dividend if valid is false
        let data = array
            .raw_iter()
            .map(|item| if item.is_zero() { value } else { *item });

        T::from_data(data, valid)
    }

    // all valid dividend case
    Some(match array {
        ArrayImpl::Int16(array) => {
            let array = f(array, valid, 1);
            ArrayImpl::Int16(Arc::new(array))
        }
        ArrayImpl::Int32(array) => {
            let array = f(array, valid, 1);
            ArrayImpl::Int32(Arc::new(array))
        }
        ArrayImpl::Int64(array) => {
            let array = f(array, valid, 1);
            ArrayImpl::Int64(Arc::new(array))
        }
        ArrayImpl::Float64(array) => {
            let array = f(array, valid, 1.0.into());
            ArrayImpl::Float64(Arc::new(array))
        }
        ArrayImpl::Decimal(array) => {
            let array = f(array, valid, Decimal::new(1, 0));
            ArrayImpl::Decimal(Arc::new(array))
        }
        _ => return None,
    })
}

fn binary_op<A, B, O, F>(a: &A, b: &B, f: F) -> O
where
    A: ArrayValidExt,
    B: ArrayValidExt,
    O: ArrayFromDataExt,
    F: Fn(&A::Item, &B::Item) -> <O::Item as ToOwned>::Owned,
{
    assert_eq!(a.len(), b.len());
    let it = a.raw_iter().zip(b.raw_iter()).map(|(a, b)| f(a, b));
    let valid = a.get_valid_bitmap().and(b.get_valid_bitmap());
    O::from_data(it, valid)
}

fn unary_op<A, O, F, V>(a: &A, f: F) -> O
where
    A: ArrayValidExt,
    O: ArrayFromDataExt,
    V: Borrow<O::Item>,
    F: Fn(&A::Item) -> V,
{
    O::from_data(a.raw_iter().map(f), a.get_valid_bitmap().clone())
}

fn try_unary_op<A, O, F, V, E>(a: &A, f: F) -> std::result::Result<O, E>
where
    A: Array,
    O: Array,
    V: Borrow<O::Item>,
    F: Fn(&A::Item) -> std::result::Result<V, E>,
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

fn select_op<A>(s: &BoolArray, a: &A, b: &A) -> A
where
    A: ArrayValidExt + ArrayFromDataExt,
{
    assert_eq!(a.len(), b.len());
    let it = a
        .raw_iter()
        .zip(b.raw_iter())
        .zip(s.raw_iter())
        .map(|((a, b), s)| if *s { a } else { b });
    let mut valid = s.get_valid_bitmap().and(a.get_valid_bitmap());
    valid.or(&s.get_valid_bitmap().not_then_and(b.get_valid_bitmap()));
    A::from_data(it, valid)
}

fn ternary_op<A, B, C, O, F, V>(a: &A, b: &B, c: &C, f: F) -> O
where
    A: Array,
    B: Array,
    C: Array,
    O: Array,
    V: Borrow<O::Item>,
    F: Fn(&A::Item, &B::Item, &C::Item) -> V,
{
    let mut builder = O::Builder::with_capacity(a.len());
    for e in a.iter().zip(b.iter()).zip(c.iter()) {
        if let ((Some(a), Some(b)), Some(c)) = e {
            builder.push(Some(f(a, b, c).borrow()));
        } else {
            builder.push(None);
        }
    }
    builder.finish()
}

/// Optimized operations.
///
/// Assume both bitvecs have the same length.
pub trait BitVecExt {
    /// self & other
    fn and(&self, other: &Self) -> Self;
    /// self |= other
    fn or(&mut self, other: &Self);
    /// !self & other
    fn not_then_and(&self, other: &Self) -> Self;
    /// Creates a [`BitVec`] from `&[bool]`.
    fn from_bool_slice(bools: &[bool]) -> Self;
}

impl BitVecExt for BitVec {
    fn and(&self, other: &Self) -> Self {
        let mut res: BitVec = (self.as_raw_slice().iter())
            .zip(other.as_raw_slice())
            .map(|(a, b)| a & b)
            .collect();
        unsafe { res.set_len(self.len()) };
        res
    }

    fn or(&mut self, other: &Self) {
        for (a, b) in self.as_raw_mut_slice().iter_mut().zip(other.as_raw_slice()) {
            *a |= b;
        }
    }

    fn not_then_and(&self, other: &Self) -> Self {
        let mut res: BitVec = (self.as_raw_slice().iter())
            .zip(other.as_raw_slice())
            .map(|(a, b)| !a & b)
            .collect();
        unsafe { res.set_len(self.len()) };
        res
    }

    fn from_bool_slice(bools: &[bool]) -> Self {
        // use SIMD to speed up
        let mut iter = bools.array_chunks::<64>();
        let mut bitvec = Vec::with_capacity((bools.len() + 63) / 64);
        for chunk in iter.by_ref() {
            let bitmask = std::simd::Mask::<i8, 64>::from_array(*chunk).to_bitmask() as usize;
            bitvec.push(bitmask);
        }
        if !iter.remainder().is_empty() {
            let mut bitmask = 0;
            for (i, b) in iter.remainder().iter().enumerate() {
                bitmask |= (*b as usize) << i;
            }
            bitvec.push(bitmask);
        }
        let mut bitvec = BitVec::from_vec(bitvec);
        bitvec.truncate(bools.len());
        bitvec
    }
}
