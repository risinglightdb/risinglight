use super::*;
use crate::types::{DataTypeKind, DataType};

#[derive(PartialEq, Clone, Debug)]
pub struct BoundIsNull {
    pub bound_expr: Box<BoundExpr>
}

impl Binder {
    pub fn bind_isnull(&mut self, expr: &Expr) -> Result<BoundExpr, BindError> {
        let bound_expr = self.bind_expr(expr)?;
        Ok(BoundExpr {
            kind: BoundExprKind::IsNull(BoundIsNull {
                bound_expr: Box::new(bound_expr)
            }),
            return_type: Some(DataType::new(DataTypeKind::Boolean, false))
        })
    }
}