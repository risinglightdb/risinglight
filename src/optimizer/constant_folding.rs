use super::*;
use crate::array::ArrayImpl;
use crate::binder::{
    BoundAggCall, BoundBinaryOp, BoundExpr, BoundExprKind, BoundTypeCast, BoundUnaryOp,
};
use std::vec::Vec;

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
        match expr.kind {
            BoundExprKind::BinaryOp(binary_op) => {
                let new_left_expr = self.rewrite_expr(*binary_op.left_expr);
                let new_right_expr = self.rewrite_expr(*binary_op.right_expr);

                let kind = if let (BoundExprKind::Constant(v1), BoundExprKind::Constant(v2)) =
                    (&new_left_expr.kind, &new_right_expr.kind)
                {
                    let res = ArrayImpl::from(v1)
                        .binary_op(&binary_op.op, &ArrayImpl::from(v2))
                        .get(0);
                    BoundExprKind::Constant(res)
                } else {
                    BoundExprKind::BinaryOp(BoundBinaryOp {
                        left_expr: Box::new(new_left_expr),
                        op: binary_op.op,
                        right_expr: Box::new(new_right_expr),
                    })
                };
                BoundExpr {
                    kind,
                    return_type: expr.return_type,
                }
            }
            BoundExprKind::UnaryOp(unary_op) => {
                let new_expr = self.rewrite_expr(*unary_op.expr);

                let kind = if let BoundExprKind::Constant(v) = &new_expr.kind {
                    let res = ArrayImpl::from(v).unary_op(&unary_op.op).get(0);
                    BoundExprKind::Constant(res)
                } else {
                    BoundExprKind::UnaryOp(BoundUnaryOp {
                        op: unary_op.op,
                        expr: Box::new(new_expr),
                    })
                };
                BoundExpr {
                    kind,
                    return_type: expr.return_type,
                }
            }
            BoundExprKind::TypeCast(cast) => {
                let new_expr = self.rewrite_expr(*cast.expr);

                let kind = if let BoundExprKind::Constant(v) = &new_expr.kind {
                    let res = ArrayImpl::from(v).try_cast(cast.ty).unwrap().get(0);
                    BoundExprKind::Constant(res)
                } else {
                    BoundExprKind::TypeCast(BoundTypeCast {
                        ty: cast.ty,
                        expr: Box::new(new_expr),
                    })
                };
                BoundExpr {
                    kind,
                    return_type: expr.return_type,
                }
            }
            BoundExprKind::AggCall(agg_call) => {
                let mut new_exprs: Vec<BoundExpr> = vec![];
                for expr in agg_call.args.into_iter() {
                    new_exprs.push(self.rewrite_expr(expr));
                }
                BoundExpr {
                    kind: BoundExprKind::AggCall(BoundAggCall {
                        kind: agg_call.kind,
                        args: new_exprs,
                        return_type: agg_call.return_type,
                    }),
                    return_type: expr.return_type,
                }
            }
            _ => expr,
        }
    }
}
