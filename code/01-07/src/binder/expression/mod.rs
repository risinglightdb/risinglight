use super::*;
use crate::parser::{Expr, Value};
use crate::types::{DataType, DataValue};

mod column_ref;

pub use self::column_ref::*;

/// A bound expression.
#[derive(Debug, PartialEq, Clone)]
pub enum BoundExpr {
    Constant(DataValue),
    ColumnRef(BoundColumnRef),
}

impl BoundExpr {
    /// Get return type of the expression.
    ///
    /// Returns `None` if the type can not be decided.
    pub fn return_type(&self) -> Option<DataType> {
        match self {
            Self::Constant(v) => v.datatype(),
            Self::ColumnRef(c) => Some(c.return_type.clone()),
        }
    }
}

impl Binder {
    /// Bind an expression.
    pub fn bind_expr(&mut self, expr: &Expr) -> Result<BoundExpr, BindError> {
        match expr {
            Expr::Value(v) => Ok(BoundExpr::Constant(v.into())),
            Expr::Identifier(ident) => self.bind_column_ref(std::slice::from_ref(ident)),
            Expr::CompoundIdentifier(idents) => self.bind_column_ref(idents),
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
