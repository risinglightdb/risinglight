use super::*;
use crate::types::DataTypeKind;

/// A bound type cast expression.
#[derive(PartialEq, Clone)]
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

impl std::fmt::Debug for BoundTypeCast {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} as {:?}", self.expr, self.ty)
    }
}
