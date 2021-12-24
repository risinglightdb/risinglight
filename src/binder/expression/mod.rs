use bitvec::prelude::BitVec;

use super::*;
use crate::catalog::ColumnRefId;
use crate::parser::{Expr, Function, UnaryOperator, Value};
use crate::types::{DataType, DataTypeExt, DataTypeKind, DataValue};

mod agg_call;
mod binary_op;
mod column_ref;
mod input_ref;
mod isnull;
mod type_cast;
mod unary_op;

pub use self::agg_call::*;
pub use self::binary_op::*;
pub use self::column_ref::*;
pub use self::input_ref::*;
pub use self::isnull::*;
pub use self::type_cast::*;
pub use self::unary_op::*;

/// A bound expression.
#[derive(PartialEq, Clone)]
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
        }
    }

    pub fn get_filter_column(&self, filter_column: &mut BitVec) {
        // TODO: match other conditions
        match self {
            Self::InputRef(expr) => filter_column.set(expr.index, true),
            Self::BinaryOp(expr) => {
                expr.left_expr.get_filter_column(filter_column);
                expr.right_expr.get_filter_column(filter_column);
            },
            Self::IsNull(expr) => expr.expr.get_filter_column(filter_column),
            _ => return,
        }
    }
}

impl std::fmt::Debug for BoundExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constant(expr) => write!(f, "{:?} (const)", expr)?,
            Self::ColumnRef(expr) => write!(f, "Column #{:?}", expr)?,
            Self::BinaryOp(expr) => write!(f, "{:?}", expr)?,
            Self::UnaryOp(expr) => write!(f, "{:?}", expr)?,
            Self::TypeCast(expr) => write!(f, "{:?} (cast)", expr)?,
            Self::AggCall(expr) => write!(f, "{:?} (agg)", expr)?,
            Self::InputRef(expr) => write!(f, "InputRef #{:?}", expr)?,
            Self::IsNull(expr) => write!(f, "{:?} (isnull)", expr)?,
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
            _ => todo!("bind expression: {:?}", expr),
        }
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
            _ => todo!("parse value: {:?}", v),
        }
    }
}
