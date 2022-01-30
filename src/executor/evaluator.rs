// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! Apply expressions on data chunks.

use std::borrow::Borrow;

use crate::array::*;
use crate::binder::BoundExpr;
use crate::parser::{BinaryOperator, UnaryOperator};
use crate::types::{ConvertError, DataTypeKind, DataValue, Date};

impl BoundExpr {
    /// Evaluate the given expression as a constant value.
    ///
    /// This method is used in the evaluation of `insert values` and optimizer
    pub fn eval(&self) -> DataValue {
        use DataValue::*;
        match &self {
            BoundExpr::Constant(v) => v.clone(),
            BoundExpr::UnaryOp(v) => match (&v.op, v.expr.eval()) {
                (UnaryOperator::Minus, Int32(i)) => Int32(-i),
                (UnaryOperator::Minus, Float64(f)) => Float64(-f),
                (UnaryOperator::Minus, Decimal(d)) => Decimal(-d),
                _ => todo!("evaluate expression: {:?}", self),
            },
            BoundExpr::BinaryOp(v) => match (&v.op, v.left_expr.eval(), v.right_expr.eval()) {
                (BinaryOperator::Plus, Int32(l), Int32(r)) => Int32(l + r),
                (BinaryOperator::Plus, Float64(l), Float64(r)) => Float64(l + r),
                (BinaryOperator::Minus, Int32(l), Int32(r)) => Int32(l - r),
                (BinaryOperator::Minus, Float64(l), Float64(r)) => Float64(l - r),
                (BinaryOperator::Multiply, Int32(l), Int32(r)) => Int32(l * r),
                (BinaryOperator::Multiply, Float64(l), Float64(r)) => Float64(l * r),
                (BinaryOperator::Divide, Int32(l), Int32(r)) => Int32(l / r),
                (BinaryOperator::Divide, Float64(l), Float64(r)) => Float64(l / r),
                _ => todo!("evaluate expression: {:?}", self),
            },
            _ => todo!("evaluate expression: {:?}", self),
        }
    }

    /// Evaluate the given expression as an array.
    pub fn eval_array(&self, chunk: &DataChunk) -> Result<ArrayImpl, ConvertError> {
        match &self {
            BoundExpr::InputRef(input_ref) => Ok(chunk.array_at(input_ref.index).clone()),
            BoundExpr::BinaryOp(binary_op) => {
                let left = binary_op.left_expr.eval_array(chunk)?;
                let right = binary_op.right_expr.eval_array(chunk)?;
                Ok(left.binary_op(&binary_op.op, &right))
            }
            BoundExpr::UnaryOp(op) => {
                let array = op.expr.eval_array(chunk)?;
                Ok(array.unary_op(&op.op))
            }
            BoundExpr::Constant(v) => {
                let mut builder = ArrayBuilderImpl::with_capacity(
                    chunk.cardinality(),
                    &self.return_type().unwrap(),
                );
                // TODO: optimize this
                for _ in 0..chunk.cardinality() {
                    builder.push(v);
                }
                Ok(builder.finish())
            }
            BoundExpr::TypeCast(cast) => {
                let array = cast.expr.eval_array(chunk)?;
                if self.return_type() == cast.expr.return_type() {
                    return Ok(array);
                }
                array.try_cast(cast.ty.clone())
            }
            BoundExpr::IsNull(expr) => {
                let array = expr.expr.eval_array(chunk)?;
                Ok(ArrayImpl::Bool(
                    (0..array.len())
                        .map(|i| array.get(i) == DataValue::Null)
                        .collect(),
                ))
            }
            BoundExpr::ExprWithAlias(expr_with_alias) => expr_with_alias.expr.eval_array(chunk),
            _ => panic!("{:?} should not be evaluated in `eval_array`", self),
        }
    }

    /// Evaluate the given expression as an array in storage engine.
    pub fn eval_array_in_storage(
        &self,
        chunk: &PackedVec<Option<ArrayImpl>>,
        cardinality: usize,
    ) -> Result<ArrayImpl, ConvertError> {
        match &self {
            BoundExpr::InputRef(input_ref) => Ok(chunk[input_ref.index].clone().unwrap()),
            BoundExpr::BinaryOp(binary_op) => {
                let left = binary_op
                    .left_expr
                    .eval_array_in_storage(chunk, cardinality)?;
                let right = binary_op
                    .right_expr
                    .eval_array_in_storage(chunk, cardinality)?;
                Ok(left.binary_op(&binary_op.op, &right))
            }
            BoundExpr::UnaryOp(op) => {
                let array = op.expr.eval_array_in_storage(chunk, cardinality)?;
                Ok(array.unary_op(&op.op))
            }
            BoundExpr::Constant(v) => {
                let mut builder =
                    ArrayBuilderImpl::with_capacity(cardinality, &self.return_type().unwrap());
                // TODO: optimize this
                for _ in 0..cardinality {
                    builder.push(v);
                }
                Ok(builder.finish())
            }
            BoundExpr::TypeCast(cast) => {
                let array = cast.expr.eval_array_in_storage(chunk, cardinality)?;
                if self.return_type() == cast.expr.return_type() {
                    return Ok(array);
                }
                array.try_cast(cast.ty.clone())
            }
            BoundExpr::IsNull(expr) => {
                let array = expr.expr.eval_array_in_storage(chunk, cardinality)?;
                Ok(ArrayImpl::Bool(
                    (0..array.len())
                        .map(|i| array.get(i) == DataValue::Null)
                        .collect(),
                ))
            }
            _ => panic!("{:?} should not be evaluated in `eval_array`", self),
        }
    }
}

impl ArrayImpl {
    /// Perform unary operation.
    pub fn unary_op(&self, op: &UnaryOperator) -> ArrayImpl {
        type A = ArrayImpl;
        match op {
            UnaryOperator::Plus => match self {
                A::Int32(_) => self.clone(),
                A::Float64(_) => self.clone(),
                A::Decimal(_) => self.clone(),
                _ => panic!("+ can only be applied to Int, Float or Decimal array"),
            },
            UnaryOperator::Minus => match self {
                A::Int32(a) => A::Int32(unary_op(a, |v| -v)),
                A::Float64(a) => A::Float64(unary_op(a, |v| -v)),
                A::Decimal(a) => A::Decimal(unary_op(a, |v| -v)),
                _ => panic!("- can only be applied to Int, Float or Decimal array"),
            },
            UnaryOperator::Not => match self {
                A::Bool(a) => A::Bool(unary_op(a, |b| !b)),
                _ => panic!("Not can only be applied to BOOL array"),
            },
            _ => todo!("evaluate operator: {:?}", op),
        }
    }

    /// Perform binary operation.
    pub fn binary_op(&self, op: &BinaryOperator, right: &ArrayImpl) -> ArrayImpl {
        type A = ArrayImpl;
        macro_rules! arith {
            ($op:tt) => {
                match (self, right) {
                    #[cfg(feature = "simd")]
                    (A::Int32(a), A::Int32(b)) => A::Int32(simd_op::<_, _, _, 32>(a, b, |a, b| a $op b)),
                    #[cfg(feature = "simd")]
                    (A::Float64(a), A::Float64(b)) => A::Float64(simd_op::<_, _, _, 32>(a, b, |a, b| a $op b)),

                    #[cfg(not(feature = "simd"))]
                    (A::Int32(a), A::Int32(b)) => A::Int32(binary_op(a, b, |a, b| a $op b)),
                    #[cfg(not(feature = "simd"))]
                    (A::Float64(a), A::Float64(b)) => A::Float64(binary_op(a, b, |a, b| a $op b)),

                    (A::Decimal(a), A::Decimal(b)) => A::Decimal(binary_op(a, b, |a, b| a $op b)),
                    (A::Date(a), A::Interval(b)) => A::Date(binary_op(a, b, |a, b| *a $op *b)),
                    _ => todo!("Support more types for {}", stringify!($op)),
                }
            }
        }
        macro_rules! cmp {
            ($op:tt) => {
                match (self, right) {
                    (A::Bool(a), A::Bool(b)) => A::Bool(binary_op(a, b, |a, b| a $op b)),
                    (A::Int32(a), A::Int32(b)) => A::Bool(binary_op(a, b, |a, b| a $op b)),
                    #[allow(clippy::float_cmp)]
                    (A::Float64(a), A::Float64(b)) => A::Bool(binary_op(a, b, |a, b| a $op b)),
                    (A::Utf8(a), A::Utf8(b)) => A::Bool(binary_op(a, b, |a, b| a $op b)),
                    (A::Date(a), A::Date(b)) => A::Bool(binary_op(a, b, |a, b| a $op b)),
                    (A::Decimal(a), A::Decimal(b)) => A::Bool(binary_op(a, b, |a, b| a $op b)),
                    _ => todo!("Support more types for {}", stringify!($op)),
                }
            }
        }
        match op {
            BinaryOperator::Plus => arith!(+),
            BinaryOperator::Minus => arith!(-),
            BinaryOperator::Multiply => arith!(*),
            BinaryOperator::Divide => arith!(/),
            BinaryOperator::Modulo => arith!(%),
            BinaryOperator::Eq => cmp!(==),
            BinaryOperator::NotEq => cmp!(!=),
            BinaryOperator::Gt => cmp!(>),
            BinaryOperator::Lt => cmp!(<),
            BinaryOperator::GtEq => cmp!(>=),
            BinaryOperator::LtEq => cmp!(<=),
            BinaryOperator::And => match (self, right) {
                (A::Bool(a), A::Bool(b)) => {
                    A::Bool(binary_op_with_null(a, b, |a, b| match (a, b) {
                        (Some(a), Some(b)) => Some(*a && *b),
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        _ => None,
                    }))
                }
                _ => panic!("And can only be applied to BOOL arrays"),
            },
            BinaryOperator::Or => match (self, right) {
                (A::Bool(a), A::Bool(b)) => {
                    A::Bool(binary_op_with_null(a, b, |a, b| match (a, b) {
                        (Some(a), Some(b)) => Some(*a || *b),
                        (Some(true), _) | (_, Some(true)) => Some(true),
                        _ => None,
                    }))
                }
                _ => panic!("Or can only be applied to BOOL arrays"),
            },
            _ => todo!("evaluate operator: {:?}", op),
        }
    }

    /// Cast the array to another type.
    pub fn try_cast(&self, data_type: DataTypeKind) -> Result<Self, ConvertError> {
        type Type = DataTypeKind;
        Ok(match self {
            Self::Bool(a) => match data_type {
                Type::Boolean => Self::Bool(a.clone()),
                Type::Int(_) => Self::Int32(unary_op(a, |&b| b as i32)),
                Type::Float(_) | Type::Double => Self::Float64(unary_op(a, |&b| b as u8 as f64)),
                Type::String | Type::Char(_) | Type::Varchar(_) => {
                    Self::Utf8(unary_op(a, |&b| if b { "true" } else { "false" }))
                }
                Type::Decimal(_, _) => Self::Decimal(unary_op(a, |&b| Decimal::from(b as u8))),
                Type::Date => return Err(ConvertError::ToDateError(Type::Boolean)),
                _ => todo!("cast array"),
            },
            Self::Int32(a) => match data_type {
                Type::Boolean => Self::Bool(unary_op(a, |&i| i != 0)),
                Type::Int(_) => Self::Int32(a.clone()),
                Type::Float(_) | Type::Double => Self::Float64(unary_op(a, |&i| i as f64)),
                Type::String | Type::Char(_) | Type::Varchar(_) => {
                    Self::Utf8(unary_op(a, |&i| i.to_string()))
                }
                Type::Decimal(_, _) => Self::Decimal(unary_op(a, |&i| Decimal::from(i))),
                Type::Date => return Err(ConvertError::ToDateError(Type::Int(None))),
                _ => todo!("cast array"),
            },
            Self::Int64(a) => match data_type {
                Type::Boolean => Self::Bool(unary_op(a, |&i| i != 0)),
                Type::Int(_) => Self::Int64(a.clone()),
                Type::Float(_) | Type::Double => Self::Float64(unary_op(a, |&i| i as f64)),
                Type::String | Type::Char(_) | Type::Varchar(_) => {
                    Self::Utf8(unary_op(a, |&i| i.to_string()))
                }
                Type::Decimal(_, _) => Self::Decimal(unary_op(a, |&i| Decimal::from(i))),
                Type::Date => return Err(ConvertError::ToDateError(Type::BigInt(None))),
                _ => todo!("cast array"),
            },
            Self::Float64(a) => match data_type {
                Type::Boolean => Self::Bool(unary_op(a, |&f| f != 0.0)),
                Type::Int(_) => Self::Int32(unary_op(a, |&f| f as i32)),
                Type::Float(_) | Type::Double => Self::Float64(a.clone()),
                Type::String | Type::Char(_) | Type::Varchar(_) => {
                    Self::Utf8(unary_op(a, |&f| f.to_string()))
                }
                Type::Decimal(_, scale) => {
                    Self::Decimal(try_unary_op(a, |&f| match Decimal::from_f64_retain(f) {
                        Some(mut d) => {
                            if let Some(s) = scale {
                                d.rescale(s as u32);
                            }
                            Ok(d)
                        }
                        None => Err(ConvertError::ToDecimalError(DataValue::Float64(f))),
                    })?)
                }
                Type::Date => return Err(ConvertError::ToDateError(Type::Double)),
                _ => todo!("cast array"),
            },
            Self::Utf8(a) => match data_type {
                Type::Boolean => Self::Bool(try_unary_op(a, |s| {
                    s.parse::<bool>()
                        .map_err(|e| ConvertError::ParseBool(s.to_string(), e))
                })?),
                Type::Int(_) => Self::Int32(try_unary_op(a, |s| {
                    s.parse::<i32>()
                        .map_err(|e| ConvertError::ParseInt(s.to_string(), e))
                })?),
                Type::Float(_) | Type::Double => Self::Float64(try_unary_op(a, |s| {
                    s.parse::<f64>()
                        .map_err(|e| ConvertError::ParseFloat(s.to_string(), e))
                })?),
                Type::String | Type::Char(_) | Type::Varchar(_) => Self::Utf8(a.clone()),
                Type::Decimal(_, _) => Self::Decimal(try_unary_op(a, |s| {
                    Decimal::from_str(s).map_err(|e| ConvertError::ParseDecimal(s.to_string(), e))
                })?),
                Type::Date => Self::Date(try_unary_op(a, |s| {
                    Date::from_str(s).map_err(|e| ConvertError::ParseDate(s.to_string(), e))
                })?),
                _ => todo!("cast array"),
            },
            Self::Blob(_) => todo!("cast array"),
            Self::Decimal(a) => match data_type {
                Type::Boolean => Self::Bool(unary_op(a, |&d| d != Decimal::from(0_i32))),
                Type::Int(_) => Self::Int32(try_unary_op(a, |&d| {
                    d.to_i32().ok_or(ConvertError::FromDecimalError(
                        DataTypeKind::Int(None),
                        DataValue::Decimal(d),
                    ))
                })?),
                Type::Float(_) | Type::Double => Self::Float64(try_unary_op(a, |&d| {
                    d.to_f64().ok_or(ConvertError::FromDecimalError(
                        DataTypeKind::Double,
                        DataValue::Decimal(d),
                    ))
                })?),
                Type::String | Type::Char(_) | Type::Varchar(_) => {
                    Self::Utf8(unary_op(a, |d| d.to_string()))
                }
                Type::Decimal(_, _) => Self::Decimal(a.clone()),
                Type::Date => return Err(ConvertError::ToDateError(Type::Decimal(None, None))),
                _ => todo!("cast array"),
            },
            Self::Date(a) => match data_type {
                Type::String | Type::Char(_) | Type::Varchar(_) => {
                    Self::Utf8(unary_op(a, |&d| d.to_string()))
                }
                ty => return Err(ConvertError::FromDateError(ty)),
            },
            Self::Interval(_) => return Err(ConvertError::FromIntervalError(data_type)),
        })
    }
}

use std::simd::{LaneCount, Simd, SimdElement, SupportedLaneCount};

use num_traits::ToPrimitive;
use rust_decimal::prelude::FromStr;
use rust_decimal::Decimal;

use crate::storage::PackedVec;
use crate::types::NativeType;

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
