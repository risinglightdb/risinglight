use super::*;
use crate::catalog::ColumnRefId;
use crate::parser::{Expr, Function, Value};
use crate::types::{DataType, DataValue};

mod agg_call;
mod binary_op;
mod column_ref;
mod type_cast;
mod unary_op;

pub use self::agg_call::*;
pub use self::binary_op::*;
pub use self::column_ref::*;
pub use self::type_cast::*;
pub use self::unary_op::*;

/// A bound expression.
#[derive(PartialEq, Clone)]
pub struct BoundExpr {
    /// The content of the expression.
    pub kind: BoundExprKind,
    /// The return type of the expression.
    ///
    /// `None` means NULL.
    pub return_type: Option<DataType>,
}

#[derive(PartialEq, Clone)]
pub enum BoundExprKind {
    Constant(DataValue),
    ColumnRef(BoundColumnRef),
    BinaryOp(BoundBinaryOp),
    UnaryOp(BoundUnaryOp),
    TypeCast(BoundTypeCast),
    AggCall(BoundAggCall),
}

impl std::fmt::Debug for BoundExprKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BoundExprKind::Constant(expr) => write!(f, "{:?} (const)", expr)?,
            BoundExprKind::ColumnRef(expr) => write!(f, "#{:?}", expr)?,
            BoundExprKind::BinaryOp(expr) => write!(f, "{:?}", expr)?,
            BoundExprKind::UnaryOp(expr) => write!(f, "{:?}", expr)?,
            BoundExprKind::TypeCast(expr) => write!(f, "{:?} (cast)", expr)?,
            BoundExprKind::AggCall(expr) => write!(f, "{:?} (agg)", expr)?,
        }
        Ok(())
    }
}

impl BoundExpr {
    /// Construct a constant value expression.
    pub fn constant(value: DataValue) -> Self {
        BoundExpr {
            return_type: value.data_type(),
            kind: BoundExprKind::Constant(value),
        }
    }
}

impl std::fmt::Debug for BoundExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.kind)?;
        if let Some(return_type) = &self.return_type {
            write!(f, " -> {:?}", return_type)?;
        }
        Ok(())
    }
}

impl Binder {
    /// Bind an expression.
    pub fn bind_expr(&mut self, expr: &Expr) -> Result<BoundExpr, BindError> {
        match expr {
            Expr::Value(v) => Ok(BoundExpr::constant(v.into())),
            Expr::Identifier(ident) => self.bind_column_ref(std::slice::from_ref(ident)),
            Expr::CompoundIdentifier(idents) => self.bind_column_ref(idents),
            Expr::BinaryOp { left, op, right } => self.bind_binary_op(left, op, right),
            Expr::UnaryOp { op, expr } => self.bind_unary_op(op, expr),
            Expr::Nested(expr) => self.bind_expr(expr),
            Expr::Cast { expr, data_type } => self.bind_type_cast(expr, data_type.clone()),
            Expr::Function(func) => self.bind_function(func),
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
