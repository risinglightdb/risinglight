use super::*;
use crate::array::{ArrayBuilderImpl, ArrayImpl};
use crate::binder::BoundBinaryOp;
use crate::binder::{BoundExpr, BoundExprKind, BoundUnaryOp};
use std::vec::Vec;
/// Constant folding rule aims to evalute the constant expression before query execution.
/// For example,
/// select 3 * 2 * a from t where a >= 100 * 300;
/// The rule will convert it into
/// select 6 * a from t  where a >= 30000;
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
    fn extract_data_value_to_array(&mut self, expr: &BoundExpr) -> ArrayImpl {
        match &expr.kind {
            BoundExprKind::Constant(value) => {
                let mut builder = ArrayBuilderImpl::from_type_of_value(value);
                builder.push(value);
                builder.finish()
            }
            _ => panic!("Cannot extract data value from other expression"),
        }
    }

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

                if left_flag && right_flag {
                    let left_arr = self.extract_data_value_to_array(&new_left_expr);
                    let right_arr = self.extract_data_value_to_array(&new_right_expr);
                    let res = left_arr.binary_op(&binary_op.op, &right_arr);
                    BoundExpr {
                        kind: BoundExprKind::Constant(res.get(0)),
                        return_type: expr.return_type,
                    }
                } else {
                    BoundExpr {
                        kind: BoundExprKind::BinaryOp(BoundBinaryOp {
                            left_expr: Box::new(new_left_expr),
                            op: binary_op.op,
                            right_expr: Box::new(new_right_expr),
                        }),
                        return_type: expr.return_type,
                    }
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
        match &expr.kind {
            BoundExprKind::Constant(_) => true,
            BoundExprKind::ColumnRef(_) => false,
            BoundExprKind::BinaryOp(binary_op) => {
                self.is_static_evaluable(&binary_op.left_expr)
                    && self.is_static_evaluable(&binary_op.right_expr)
            }
            BoundExprKind::UnaryOp(unary_op) => self.is_static_evaluable(&unary_op.expr),
            BoundExprKind::TypeCast(_) => false,
            BoundExprKind::AggCall(_) => false,
        }
    }
}
