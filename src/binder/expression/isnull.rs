// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use serde::Serialize;

use super::*;

#[derive(PartialEq, Clone, Debug, Serialize)]
pub struct BoundIsNull {
    pub expr: Box<BoundExpr>,
}

impl Binder {
    pub fn bind_isnull(&mut self, expr: &Expr) -> Result<BoundExpr, BindError> {
        let bound_expr = self.bind_expr(expr)?;
        Ok(BoundExpr::IsNull(BoundIsNull {
            expr: Box::new(bound_expr),
        }))
    }
}
