use super::*;
use crate::catalog::ColumnRefId;
use crate::parser::{Expr, Value};
use crate::types::{DataType, DataValue};

mod column_ref;

pub use self::column_ref::*;

#[derive(Debug, PartialEq, Clone)]
pub struct BoundExpr {
    pub kind: BoundExprKind,
    /// The return type of the expression.
    /// `None` means NULL.
    pub return_type: Option<DataType>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum BoundExprKind {
    Constant(DataValue),
    ColumnRef(BoundColumnRef),
}

impl Binder {
    pub fn bind_expr(&mut self, expr: &Expr) -> Result<BoundExpr, BindError> {
        match expr {
            Expr::Value(v) => Ok(BoundExpr {
                kind: BoundExprKind::Constant(v.into()),
                return_type: DataValue::from(v).data_type(),
            }),
            Expr::Identifier(ident) => self.bind_column_ref(std::slice::from_ref(ident)),
            Expr::CompoundIdentifier(idents) => self.bind_column_ref(idents),
            _ => todo!("bind expression"),
        }
    }
}

impl From<&Value> for DataValue {
    fn from(v: &Value) -> Self {
        match v {
            // FIXME: float?
            Value::Number(n, _) => Self::Int32(n.parse().unwrap()),
            Value::SingleQuotedString(s) => Self::String(s.clone()),
            Value::DoubleQuotedString(s) => Self::String(s.clone()),
            Value::Boolean(b) => Self::Bool(*b),
            Value::Null => Self::Null,
            _ => todo!("parse value"),
        }
    }
}
