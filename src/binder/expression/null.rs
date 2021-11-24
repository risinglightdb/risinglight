use super::*;
use crate::types::{DataType, DataTypeKind};

#[derive(PartialEq, Clone, Debug)]
pub struct BoundIsNull {
    pub expr: Box<BoundExpr>,
}

impl Binder {
    pub fn bind_isnull(&mut self, expr: &Expr) -> Result<BoundExpr, BindError> {
        let bound_expr = self.bind_expr(expr)?;
        Ok(BoundExpr {
            kind: BoundExprKind::IsNull(BoundIsNull {
                expr: Box::new(bound_expr),
            }),
            return_type: Some(DataType::new(DataTypeKind::Boolean, false)),
        })
    }
}
