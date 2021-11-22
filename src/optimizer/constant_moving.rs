use super::*;
use crate::binder::{BoundBinaryOp, BoundExpr, BoundExprKind::*};
use crate::parser::BinaryOperator::*;
use crate::types::DataValue;
pub struct ConstantMovingRule;
use crate::binder::BoundExprKind;
/// Constant moving rule moves constants in the filtering conditions from one side to the other
/// side. NOTICE: we don't process division as it is complicated.
/// x / 2 == 2 means x = 4 or x = 5 !!!
/// For example,
/// `select a from t where 100 + a > 300;`
/// The rule will convert it into
/// `select a from t where a > 200;`
impl PlanRewriter for ConstantMovingRule {
    fn rewrite_filter(&mut self, plan: LogicalFilter) -> LogicalPlan {
        let new_expr = self.rewrite_expr(plan.expr);
        LogicalPlan::Filter(LogicalFilter {
            expr: new_expr,
            child: Box::new(*plan.child),
        })
    }
}

macro_rules! constant_moving_rule {
    ( $($t: path), *) => {
        pub fn rewrite_expr(&mut self, expr: BoundExpr) -> BoundExpr {
            let new_kind = match &expr.kind {
                BinaryOp(op) => match (&op.op, &op.left_expr.kind, &op.right_expr.kind) {
                    $(
                    (Eq, BinaryOp(bin_op), Constant($t(rval)))
                    | (NotEq, BinaryOp(bin_op), Constant($t(rval)))
                    | (Gt, BinaryOp(bin_op), Constant($t(rval)))
                    | (Lt, BinaryOp(bin_op), Constant($t(rval)))
                    | (GtEq, BinaryOp(bin_op), Constant($t(rval)))
                    | (LtEq, BinaryOp(bin_op), Constant($t(rval))) => {
                        match (&bin_op.op, &bin_op.left_expr.kind, &bin_op.right_expr.kind) {
                            (Plus, other, Constant($t(lval)))
                            | (Plus, Constant($t(lval)), other) => {
                                BoundExprKind::BinaryOp(BoundBinaryOp {
                                    left_expr: Box::new(BoundExpr {
                                        kind: other.clone(),
                                        return_type: op.left_expr.return_type.clone(),
                                    }),
                                    op: op.op.clone(),
                                    right_expr: Box::new(BoundExpr {
                                        kind: BoundExprKind::Constant($t(rval - lval)),
                                        return_type: op.right_expr.return_type.clone(),
                                    }),
                                })
                            }
                            (Minus, other, Constant($t(lval)))
                            | (Minus, Constant($t(lval)), other) => {
                                BoundExprKind::BinaryOp(BoundBinaryOp {
                                    left_expr: Box::new(BoundExpr {
                                        kind: other.clone(),
                                        return_type: op.left_expr.return_type.clone(),
                                    }),
                                    op: op.op.clone(),
                                    right_expr: Box::new(BoundExpr {
                                        kind: BoundExprKind::Constant($t(rval + lval)),
                                        return_type: op.right_expr.return_type.clone(),
                                    }),
                                })
                            }
                            (Multiply, other, Constant($t(lval)))
                            | (Multiply, Constant($t(lval)), other) => {
                                BoundExprKind::BinaryOp(BoundBinaryOp {
                                    left_expr: Box::new(BoundExpr {
                                        kind: other.clone(),
                                        return_type: op.left_expr.return_type.clone(),
                                    }),
                                    op: op.op.clone(),
                                    right_expr: Box::new(BoundExpr {
                                        kind: BoundExprKind::Constant($t(rval / lval)),
                                        return_type: op.right_expr.return_type.clone(),
                                    }),
                                })
                            }
                            _ => expr.kind.clone(),
                        }
                    },)*
                    _ => expr.kind.clone(),
                },
                _ => expr.kind.clone(),
            };
            BoundExpr {
                kind: new_kind,
                return_type: expr.return_type,
            }
        }
    };
}

impl ConstantMovingRule {
    constant_moving_rule!(DataValue::Int32, DataValue::Float64);
}

// BoundExpr {
// kind: BoundExprKind::BinaryOp(BoundBinaryOp {
// left_expr: Box::new(BoundExpr {
// kind: other.clone(),
// return_type: op.left_expr.return_type,
// }),
// op: op.op.clone(),
// right_expr: Box::new(BoundExpr {
// kind: BoundExprKind::Constant(DataValue::Int32(rval - lval)),
// return_type: op.right_expr.return_type,
// }),
// }),
// return_type: expr.return_type
// },
// (/)
