use std::vec::Vec;

use super::*;
use crate::array::ArrayImpl;
use crate::binder::{BoundAggCall, BoundBinaryOp, BoundExpr, BoundTypeCast, BoundUnaryOp};

/// Constant folding rule aims to evalute the constant expression before query execution.
///
/// For example,
/// `select 3 * 2 * a from t where a >= 100 * 300;`
/// The rule will convert it into
/// `select 6 * a from t where a >= 30000;`
#[derive(Default)]
pub struct ConstantFolding;

impl PlanRewriter for ConstantFolding {
    fn rewrite_expr(&mut self, expr: BoundExpr) -> BoundExpr {
        use BoundExpr::*;
        match expr {
            BinaryOp(binary_op) => {
                let new_left_expr = self.rewrite_expr(*binary_op.left_expr);
                let new_right_expr = self.rewrite_expr(*binary_op.right_expr);

                if let (Constant(v1), Constant(v2)) = (&new_left_expr, &new_right_expr) {
                    let res = ArrayImpl::from(v1)
                        .binary_op(&binary_op.op, &ArrayImpl::from(v2))
                        .get(0);
                    Constant(res)
                } else {
                    BinaryOp(BoundBinaryOp {
                        op: binary_op.op,
                        left_expr: (new_left_expr.into()),
                        right_expr: (new_right_expr.into()),
                        return_type: binary_op.return_type.clone(),
                    })
                }
            }
            UnaryOp(unary_op) => {
                let new_expr = self.rewrite_expr(*unary_op.expr);

                if let Constant(v) = &new_expr {
                    let res = ArrayImpl::from(v).unary_op(&unary_op.op).get(0);
                    Constant(res)
                } else {
                    UnaryOp(BoundUnaryOp {
                        op: unary_op.op,
                        expr: (new_expr.into()),
                        return_type: unary_op.return_type.clone(),
                    })
                }
            }
            TypeCast(cast) => {
                let new_expr = self.rewrite_expr(*cast.expr);

                if let Constant(v) = &new_expr {
                    let res = ArrayImpl::from(v).try_cast(cast.ty).unwrap().get(0);
                    Constant(res)
                } else {
                    TypeCast(BoundTypeCast {
                        ty: cast.ty,
                        expr: (new_expr.into()),
                    })
                }
            }
            AggCall(agg_call) => {
                let mut new_exprs: Vec<BoundExpr> = vec![];
                for expr in agg_call.args {
                    new_exprs.push(self.rewrite_expr(expr));
                }
                AggCall(BoundAggCall {
                    kind: agg_call.kind,
                    args: new_exprs,
                    return_type: agg_call.return_type,
                })
            }
            _ => expr,
        }
    }
}
