// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use bitvec::prelude::BitVec;
use serde::Serialize;
use sqlparser::ast::BinaryOperator;

use super::*;
use crate::catalog::ColumnRefId;
use crate::parser::{DateTimeField, Expr, Function, UnaryOperator, Value};
use crate::types::{DataType, DataTypeExt, DataTypeKind, DataValue, Interval};

mod agg_call;
mod binary_op;
mod column_ref;
mod expr_with_alias;
mod input_ref;
mod isnull;
mod type_cast;
mod unary_op;

pub use self::agg_call::*;
pub use self::binary_op::*;
pub use self::column_ref::*;
pub use self::expr_with_alias::*;
pub use self::input_ref::*;
pub use self::isnull::*;
pub use self::type_cast::*;
pub use self::unary_op::*;

/// A bound expression.
#[derive(PartialEq, Clone, Serialize)]
pub enum BoundExpr {
    Constant(DataValue),
    ColumnRef(BoundColumnRef),
    /// Only used after column ref is resolved into input ref
    InputRef(BoundInputRef),
    BinaryOp(BoundBinaryOp),
    UnaryOp(BoundUnaryOp),
    TypeCast(BoundTypeCast),
    AggCall(BoundAggCall),
    IsNull(BoundIsNull),
    ExprWithAlias(BoundExprWithAlias),
    Alias(BoundAlias),
}

impl BoundExpr {
    pub fn return_type(&self) -> Option<DataType> {
        match self {
            Self::Constant(v) => v.data_type(),
            Self::ColumnRef(expr) => Some(expr.desc.datatype().clone()),
            Self::BinaryOp(expr) => expr.return_type.clone(),
            Self::UnaryOp(expr) => expr.return_type.clone(),
            Self::TypeCast(expr) => Some(expr.ty.clone().nullable()),
            Self::AggCall(expr) => Some(expr.return_type.clone()),
            Self::InputRef(expr) => Some(expr.return_type.clone()),
            Self::IsNull(_) => Some(DataTypeKind::Boolean.not_null()),
            Self::ExprWithAlias(expr) => expr.expr.return_type(),
            Self::Alias(_) => None,
        }
    }

    fn get_filter_column_inner(&self, filter_column: &mut BitVec) {
        match self {
            Self::Constant(_) => {}
            Self::ColumnRef(_) => {}
            Self::InputRef(expr) => filter_column.set(expr.index, true),
            Self::BinaryOp(expr) => {
                expr.left_expr.get_filter_column_inner(filter_column);
                expr.right_expr.get_filter_column_inner(filter_column);
            }
            Self::UnaryOp(expr) => {
                expr.expr.get_filter_column_inner(filter_column);
            }
            Self::TypeCast(expr) => {
                expr.expr.get_filter_column_inner(filter_column);
            }
            Self::AggCall(expr) => {
                for sub_expr in &expr.args {
                    sub_expr.get_filter_column_inner(filter_column);
                }
            }
            Self::IsNull(expr) => expr.expr.get_filter_column_inner(filter_column),
            Self::ExprWithAlias(expr) => {
                expr.expr.get_filter_column_inner(filter_column);
            }
            Self::Alias(_) => {}
        }
    }

    pub fn get_filter_column(&self, len: usize) -> BitVec {
        let mut filter_column = BitVec::repeat(false, len);
        self.get_filter_column_inner(&mut filter_column);
        filter_column
    }
}

impl std::fmt::Debug for BoundExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constant(expr) => write!(f, "{:?} (const)", expr)?,
            Self::ColumnRef(expr) => write!(f, "Column #{:?}", expr)?,
            Self::BinaryOp(expr) => write!(f, "{}", expr)?,
            Self::UnaryOp(expr) => write!(f, "{:?}", expr)?,
            Self::TypeCast(expr) => write!(f, "{:?}", expr)?,
            Self::AggCall(expr) => write!(f, "{:?} (agg)", expr)?,
            Self::InputRef(expr) => write!(f, "InputRef #{:?}", expr)?,
            Self::IsNull(expr) => write!(f, "{:?} (isnull)", expr)?,
            Self::ExprWithAlias(expr) => write!(f, "{:?}", expr)?,
            Self::Alias(expr) => write!(f, "{:?}", expr)?,
        }
        Ok(())
    }
}

impl std::fmt::Display for BoundExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constant(expr) => write!(f, "{}", expr)?,
            Self::ColumnRef(expr) => write!(f, "Column #{:?}", expr)?,
            Self::BinaryOp(expr) => write!(f, "{}", expr)?,
            Self::UnaryOp(expr) => write!(f, "{:?}", expr)?,
            Self::TypeCast(expr) => write!(f, "{}", expr)?,
            Self::AggCall(expr) => write!(f, "{:?} (agg)", expr)?,
            Self::InputRef(expr) => write!(f, "InputRef #{:?}", expr)?,
            Self::IsNull(expr) => write!(f, "{:?} (isnull)", expr)?,
            Self::ExprWithAlias(expr) => write!(f, "{:?}", expr)?,
            Self::Alias(expr) => write!(f, "{:?}", expr)?,
        }
        Ok(())
    }
}

impl Binder {
    /// Bind an expression.
    pub fn bind_expr(&mut self, expr: &Expr) -> Result<BoundExpr, BindError> {
        match expr {
            Expr::Value(v) => Ok(BoundExpr::Constant(v.into())),
            Expr::Identifier(ident) => self.bind_column_ref(std::slice::from_ref(ident)),
            Expr::CompoundIdentifier(idents) => self.bind_column_ref(idents),
            Expr::BinaryOp { left, op, right } => self.bind_binary_op(left, op, right),
            Expr::UnaryOp { op, expr } => self.bind_unary_op(op, expr),
            Expr::Nested(expr) => self.bind_expr(expr),
            Expr::Cast { expr, data_type } => self.bind_type_cast(expr, data_type.clone()),
            Expr::Function(func) => self.bind_function(func),
            Expr::IsNull(expr) => self.bind_isnull(expr),
            Expr::IsNotNull(expr) => {
                let expr = self.bind_isnull(expr)?;
                Ok(BoundExpr::UnaryOp(BoundUnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(expr),
                    return_type: Some(DataTypeKind::Boolean.not_null()),
                }))
            }
            Expr::TypedString { data_type, value } => self.bind_typed_string(data_type, value),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => self.bind_between(expr, negated, low, high),
            _ => todo!("bind expression: {:?}", expr),
        }
    }

    fn bind_typed_string(
        &mut self,
        data_type: &DataTypeKind,
        value: &str,
    ) -> Result<BoundExpr, BindError> {
        match data_type {
            DataTypeKind::Date => {
                let date = value.parse().map_err(|_| {
                    BindError::CastError(DataValue::String(value.into()), DataTypeKind::Date)
                })?;
                Ok(BoundExpr::Constant(DataValue::Date(date)))
            }
            t => todo!("support typed string: {:?}", t),
        }
    }

    fn bind_between(
        &mut self,
        expr: &Expr,
        negated: &bool,
        low: &Expr,
        high: &Expr,
    ) -> Result<BoundExpr, BindError> {
        use BinaryOperator::{And, Gt, GtEq, Lt, LtEq, Or};

        let (left_op, right_op, final_op) = match negated {
            false => (GtEq, LtEq, And),
            true => (Lt, Gt, Or),
        };

        let left_expr = self.bind_binary_op(expr, &left_op, low)?;
        let right_expr = self.bind_binary_op(expr, &right_op, high)?;
        Ok(BoundExpr::BinaryOp(BoundBinaryOp {
            op: final_op,
            left_expr: Box::new(left_expr),
            right_expr: Box::new(right_expr),
            return_type: Some(DataType::new(DataTypeKind::Boolean, false)),
        }))
    }
}

impl From<&Value> for DataValue {
    fn from(v: &Value) -> Self {
        match v {
            Value::Number(n, _) => {
                if let Ok(int) = n.parse::<i32>() {
                    Self::Int32(int)
                } else if let Ok(float) = n.parse::<f64>() {
                    Self::Float64(float)
                } else {
                    panic!("invalid digit: {}", n);
                }
            }
            Value::SingleQuotedString(s) => Self::String(s.clone()),
            Value::DoubleQuotedString(s) => Self::String(s.clone()),
            Value::Boolean(b) => Self::Bool(*b),
            Value::Null => Self::Null,
            Value::Interval {
                value,
                leading_field,
                ..
            } => match leading_field {
                Some(DateTimeField::Day) => {
                    Self::Interval(Interval::from_days(value.parse().unwrap()))
                }
                Some(DateTimeField::Month) => {
                    Self::Interval(Interval::from_months(value.parse().unwrap()))
                }
                Some(DateTimeField::Year) => {
                    Self::Interval(Interval::from_years(value.parse().unwrap()))
                }
                _ => todo!("Support interval with leading field: {:?}", leading_field),
            },
            _ => todo!("parse value: {:?}", v),
        }
    }
}
