use super::*;
use crate::types::DataTypeKind;

/// A bound type cast expression.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundTypeCast {
    pub expr: Box<BoundExpr>,
    pub ty: DataTypeKind,
}

impl Binder {
    pub fn bind_type_cast(
        &mut self,
        expr: &Expr,
        ty: DataTypeKind,
    ) -> Result<BoundExpr, BindError> {
        let bound_expr = self.bind_expr(expr)?;
        Ok(BoundExpr::TypeCast(BoundTypeCast {
            expr: (bound_expr.into()),
            ty,
        }))
    }
}
