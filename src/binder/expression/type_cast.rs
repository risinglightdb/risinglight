use super::*;
use crate::types::DataTypeKind;

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
                expr: Box::new(bound_expr),
                ty,
            }),
            return_type: Some(return_type),
        })
    }
}
