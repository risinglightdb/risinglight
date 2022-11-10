// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use serde::Serialize;

use super::*;
use crate::parser::UnaryOperator;

/// A bound unary operation expression.
#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct BoundUnaryOp {
    pub op: UnaryOperator,
    pub expr: Box<BoundExpr>,
    pub return_type: DataType,
}

impl Binder {
    pub fn bind_unary_op(
        &mut self,
        op: &UnaryOperator,
        expr: &Expr,
    ) -> Result<BoundExpr, BindError> {
        // use UnaryOperator as Op;
        let bound_expr = self.bind_expr(expr)?;
        Ok(BoundExpr::UnaryOp(BoundUnaryOp {
            op: op.clone(),
            // TODO: check data type
            return_type: bound_expr.return_type(),
            expr: bound_expr.into(),
        }))
    }
}
