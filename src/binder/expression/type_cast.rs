// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use serde::Serialize;

use super::*;
use crate::types::DataTypeKind;

/// A bound type cast expression.
#[derive(PartialEq, Clone, Serialize)]
pub struct BoundTypeCast {
    pub expr: Box<BoundExpr>,
    pub ty: DataTypeKind,
}

impl Binder {
    pub fn bind_type_cast(
        &mut self,
        expr: &Expr,
        mut ty: DataTypeKind,
    ) -> Result<BoundExpr, BindError> {
        let bound_expr = self.bind_expr(expr)?;
        // workaround for 'BLOB'
        if let DataTypeKind::Custom(name) = &ty {
            if name.0.len() == 1 && name.0[0].value.to_lowercase() == "blob" {
                ty = DataTypeKind::Blob(0);
            }
        }
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

impl std::fmt::Display for BoundTypeCast {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.expr)
    }
}
