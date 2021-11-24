use super::*;
use crate::parser::UnaryOperator;

/// A bound unary operation expression.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundUnaryOp {
    pub op: UnaryOperator,
    pub expr: Box<BoundExpr>,
}

impl Binder {
    pub fn bind_unary_op(
        &mut self,
        op: &UnaryOperator,
        expr: &Expr,
    ) -> Result<BoundExpr, BindError> {
        // use UnaryOperator as Op;
        let bound_expr = self.bind_expr(expr)?;
        // TODO: check data type
        let return_type = bound_expr.return_type.clone();
        Ok(BoundExpr {
            kind: BoundExprKind::UnaryOp(BoundUnaryOp {
                op: op.clone(),
                expr: (bound_expr.into()),
            }),
            return_type,
        })
    }
}
