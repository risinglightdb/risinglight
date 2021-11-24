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
        let return_type = DataType::new(
            ty.clone(),
            bound_expr.return_type.as_ref().unwrap().is_nullable(),
        );
        Ok(BoundExpr {
            kind: BoundExprKind::TypeCast(BoundTypeCast {
                expr: (bound_expr.into()),
                ty,
            }),
            return_type: Some(return_type),
        })
    }
}
