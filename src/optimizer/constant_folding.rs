use super::*;
use crate::binder::BoundBinaryOp;
use crate::binder::{BoundExpr, BoundExprKind, BoundUnaryOp};
use std::vec::Vec;
/// Constant folding rule aims to evalute the constant expression before query execution.
/// For example,
/// select 3 * 2 * a from t;
/// The rule will convert it into
/// select 6 * a from t;
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
            child: plan.child,
        })
    }
}

impl ConstantFoldingRewriter {
    fn rewrite_expression(&mut self, expr: BoundExpr) -> BoundExpr {
        match expr.kind {
            BoundExprKind::Constant(value) => BoundExpr {
                kind: BoundExprKind::Constant(value),
                return_type: expr.return_type,
            },
            BoundExprKind::ColumnRef(column_ref) => BoundExpr {
                kind: BoundExprKind::ColumnRef(column_ref),
                return_type: expr.return_type,
            },
            BoundExprKind::BinaryOp(binary_op) => {
                let new_left_expr = self.rewrite_expression(*binary_op.left_expr);
                let left_flag = self.is_static_evaluable(&new_left_expr);
                let new_right_expr = self.rewrite_expression(*binary_op.right_expr);
                let right_flag = self.is_static_evaluable(&new_right_expr);

                let new_expr = BoundExpr {
                    kind: BoundExprKind::BinaryOp(BoundBinaryOp {
                        left_expr: Box::new(new_left_expr),
                        op: binary_op.op,
                        right_expr: Box::new(new_right_expr),
                    }),
                    return_type: expr.return_type,
                };
                if left_flag && right_flag {
                    BoundExpr {
                        kind: BoundExprKind::Constant(new_expr.eval()),
                        return_type: new_expr.return_type,
                    }
                } else {
                    new_expr
                }
            }
            BoundExprKind::UnaryOp(unary_op) => {
                let new_expr = self.rewrite_expression(*unary_op.expr);
                BoundExpr {
                    kind: BoundExprKind::UnaryOp(BoundUnaryOp {
                        op: unary_op.op,
                        expr: Box::new(new_expr),
                    }),
                    return_type: expr.return_type,
                }
            }
            BoundExprKind::TypeCast(cast) => BoundExpr {
                kind: BoundExprKind::TypeCast(cast),
                return_type: expr.return_type,
            },
            BoundExprKind::AggCall(agg_call) => BoundExpr {
                kind: BoundExprKind::AggCall(agg_call),
                return_type: expr.return_type,
            },
        }
    }

    fn is_static_evaluable(&self, expr: &BoundExpr) -> bool {
        match expr.kind {
            BoundExprKind::Constant(_) => true,
            BoundExprKind::ColumnRef(_) => false,
            BoundExprKind::BinaryOp(_) => false,
            BoundExprKind::UnaryOp(_) => true,
            BoundExprKind::TypeCast(_) => false,
            BoundExprKind::AggCall(_) => false,
        }
    }
}
