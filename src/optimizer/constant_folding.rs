use super::*;
use crate::array::ArrayImpl;
use crate::binder::{BoundBinaryOp, BoundExpr, BoundExprKind, BoundUnaryOp};
use std::vec::Vec;

/// Constant folding rule aims to evalute the constant expression before query execution.
///
/// For example,
/// `select 3 * 2 * a from t where a >= 100 * 300;`
/// The rule will convert it into
/// `select 6 * a from t where a >= 30000;`
#[derive(Default)]
pub struct ConstantFoldingRewriter {}

impl PlanRewriter for ConstantFoldingRewriter {
    fn rewrite_projection(&mut self, plan: LogicalProjection) -> LogicalPlan {
        let mut new_exprs: Vec<BoundExpr> = vec![];
        for expr in plan.project_expressions.into_iter() {
            new_exprs.push(self.rewrite_expression(expr));
        }
        LogicalPlan::Projection(LogicalProjection {
            project_expressions: new_exprs,
            child: Box::new(self.rewrite_plan(*plan.child)),
        })
    }

    fn rewrite_filter(&mut self, plan: LogicalFilter) -> LogicalPlan {
        let new_expr = self.rewrite_expression(plan.expr);
        LogicalPlan::Filter(LogicalFilter {
            expr: new_expr,
            child: Box::new(self.rewrite_plan(*plan.child)),
        })
    }
}

impl ConstantFoldingRewriter {
    fn rewrite_expression(&mut self, expr: BoundExpr) -> BoundExpr {
        match expr.kind {
            BoundExprKind::BinaryOp(binary_op) => {
                let new_left_expr = self.rewrite_expression(*binary_op.left_expr);
                let new_right_expr = self.rewrite_expression(*binary_op.right_expr);

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
                let new_expr = self.rewrite_expression(*unary_op.expr);

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
            _ => expr,
        }
    }
}
