use super::*;
use crate::binder::{BoundBinaryOp, BoundExpr, BoundExprKind::*};
use crate::parser::BinaryOperator::*;

/// Constant moving rule moves constants in the filtering conditions from one side to the other
/// side.
///
/// NOTICE: we don't process division as it is complicated.
/// x / 2 == 2 means x = 4 or x = 5 !!!
/// For example,
/// `select a from t where 100 + a > 300;`
/// The rule will convert it into
/// `select a from t where a > 200;`
pub struct ConstantMovingRule;

impl PlanRewriter for ConstantMovingRule {
    fn rewrite_expr(&mut self, expr: BoundExpr) -> BoundExpr {
        let new_kind = match &expr.kind {
            BinaryOp(op) => match (&op.op, &op.left_expr.kind, &op.right_expr.kind) {
                (Eq | NotEq | Gt | Lt | GtEq | LtEq, BinaryOp(bin_op), Constant(rval)) => {
                    match (&bin_op.op, &bin_op.left_expr.kind, &bin_op.right_expr.kind) {
                        (Plus, other, Constant(lval)) | (Plus, Constant(lval), other) => {
                            BinaryOp(BoundBinaryOp {
                                left_expr: Box::new(BoundExpr {
                                    kind: other.clone(),
                                    return_type: op.left_expr.return_type.clone(),
                                }),
                                op: op.op.clone(),
                                right_expr: Box::new(BoundExpr {
                                    kind: Constant(rval - lval),
                                    return_type: op.right_expr.return_type.clone(),
                                }),
                            })
                        }
                        (Minus, other, Constant(lval)) => BinaryOp(BoundBinaryOp {
                            left_expr: Box::new(BoundExpr {
                                kind: other.clone(),
                                return_type: op.left_expr.return_type.clone(),
                            }),
                            op: op.op.clone(),
                            right_expr: Box::new(BoundExpr {
                                kind: Constant(rval + lval),
                                return_type: op.right_expr.return_type.clone(),
                            }),
                        }),
                        (Minus, Constant(lval), other) => BinaryOp(BoundBinaryOp {
                            left_expr: Box::new(BoundExpr {
                                kind: Constant(lval - rval),
                                return_type: op.left_expr.return_type.clone(),
                            }),
                            op: op.op.clone(),
                            right_expr: Box::new(BoundExpr {
                                kind: other.clone(),
                                return_type: op.right_expr.return_type.clone(),
                            }),
                        }),
                        (Multiply, other, Constant(lval)) | (Multiply, Constant(lval), other)
                            if lval.is_positive() && rval.is_divisible_by(lval) =>
                        {
                            BinaryOp(BoundBinaryOp {
                                left_expr: Box::new(BoundExpr {
                                    kind: other.clone(),
                                    return_type: op.left_expr.return_type.clone(),
                                }),
                                // TODO: flip op when lval is negative
                                op: op.op.clone(),
                                right_expr: Box::new(BoundExpr {
                                    kind: Constant(rval / lval),
                                    return_type: op.right_expr.return_type.clone(),
                                }),
                            })
                        }
                        _ => expr.kind,
                    }
                }
                _ => expr.kind,
            },
            _ => expr.kind,
        };
        BoundExpr {
            kind: new_kind,
            return_type: expr.return_type,
        }
    }
}
