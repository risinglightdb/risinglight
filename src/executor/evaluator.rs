use crate::{
    array::*,
    binder::{BoundExpr, BoundExprKind},
    parser::BinaryOperator,
    types::{DataTypeKind, DataValue},
};
use std::borrow::Borrow;

impl BoundExpr {
    /// Evaluate the given expression.
    pub fn eval(&self) -> DataValue {
        match &self.kind {
            BoundExprKind::Constant(v) => v.clone(),
            _ => todo!("evaluate expression"),
        }
    }

    pub fn eval_array(&self, chunk: &DataChunk) -> Result<ArrayImpl, ConvertError> {
        match &self.kind {
            BoundExprKind::ColumnRef(col_ref) => {
                let mut builder = ArrayBuilderImpl::new(self.return_type.clone().unwrap());
                builder.append(chunk.array_at(col_ref.column_index as usize));
                Ok(builder.finish())
            }
            BoundExprKind::BinaryOp(binary_op) => {
                let left = binary_op.left_expr.eval_array(chunk)?;
                let right = binary_op.right_expr.eval_array(chunk)?;
                Ok(left.binary_op(&binary_op.op, &right))
            }
            BoundExprKind::Constant(v) => {
                let mut builder = ArrayBuilderImpl::new(self.return_type.clone().unwrap());
                // TODO: optimize this
                for _ in 0..chunk.cardinality() {
                    builder.push(v);
                }
                Ok(builder.finish())
            }
            BoundExprKind::TypeCast(cast) => {
                let array = cast.expr.eval_array(chunk)?;
                if self.return_type == cast.expr.return_type {
                    return Ok(array);
                }
                array.try_cast(cast.ty.clone())
            }
        }
    }
}

impl ArrayImpl {
    /// Perform binary operation.
    pub fn binary_op(&self, op: &BinaryOperator, right: &ArrayImpl) -> ArrayImpl {
        type A = ArrayImpl;
        macro_rules! arith {
            ($op:tt) => {
                match (self, right) {
                    (A::Int32(a), A::Int32(b)) => A::Int32(binary_op(a, b, |a, b| a $op b)),
                    (A::Float64(a), A::Float64(b)) => A::Float64(binary_op(a, b, |a, b| a $op b)),
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
                    (A::UTF8(a), A::UTF8(b)) => A::Bool(binary_op(a, b, |a, b| a $op b)),
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
                Type::Int => Self::Int32(unary_op(a, |&b| b as i32)),
                Type::Float(_) | Type::Double => Self::Float64(unary_op(a, |&b| b as u8 as f64)),
                Type::String | Type::Char(_) | Type::Varchar(_) => {
                    Self::UTF8(unary_op(a, |&b| if b { "true" } else { "false" }))
                }
                _ => todo!("cast array"),
            },
            Self::Int32(a) => match data_type {
                Type::Boolean => Self::Bool(unary_op(a, |&i| i != 0)),
                Type::Int => Self::Int32(a.clone()),
                Type::Float(_) | Type::Double => Self::Float64(unary_op(a, |&i| i as f64)),
                Type::String | Type::Char(_) | Type::Varchar(_) => {
                    Self::UTF8(unary_op(a, |&i| i.to_string()))
                }
                _ => todo!("cast array"),
            },
            Self::Float64(a) => match data_type {
                Type::Boolean => Self::Bool(unary_op(a, |&f| f != 0.0)),
                Type::Int => Self::Int32(unary_op(a, |&f| f as i32)),
                Type::Float(_) | Type::Double => Self::Float64(a.clone()),
                Type::String | Type::Char(_) | Type::Varchar(_) => {
                    Self::UTF8(unary_op(a, |&f| f.to_string()))
                }
                _ => todo!("cast array"),
            },
            Self::UTF8(a) => match data_type {
                Type::Boolean => Self::Bool(try_unary_op(a, |s| s.parse::<bool>())?),
                Type::Int => Self::Int32(try_unary_op(a, |s| s.parse::<i32>())?),
                Type::Float(_) | Type::Double => {
                    Self::Float64(try_unary_op(a, |s| s.parse::<f64>())?)
                }
                Type::String | Type::Char(_) | Type::Varchar(_) => Self::UTF8(a.clone()),
                _ => todo!("cast array"),
            },
        })
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum ConvertError {
    #[error("failed to convert string to int")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("failed to convert string to float")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error("failed to convert string to bool")]
    ParseBool(#[from] std::str::ParseBoolError),
}

fn binary_op<A, B, O, F, V>(a: &A, b: &B, f: F) -> O
where
    A: Array,
    B: Array,
    O: Array,
    V: Borrow<O::Item>,
    F: Fn(&A::Item, &B::Item) -> V,
{
    assert_eq!(a.len(), b.len());
    let mut builder = O::Builder::new(a.len());
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
    let mut builder = O::Builder::new(a.len());
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
    let mut builder = O::Builder::new(a.len());
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
    let mut builder = O::Builder::new(a.len());
    for e in a.iter() {
        if let Some(e) = e {
            builder.push(Some(f(e)?.borrow()));
        } else {
            builder.push(None);
        }
    }
    Ok(builder.finish())
}
