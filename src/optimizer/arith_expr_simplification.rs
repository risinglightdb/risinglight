use super::*;
use crate::binder::{BoundExpr, BoundExprKind};
use crate::parser::BinaryOperator;
use crate::types::DataValue;
use std::vec::Vec;
/// Arithemtic expression simplification rule prunes the useless constant in the binary expressions.
///
/// For example,
/// select 1 * a, b / 1, c + 0, d - 0 from t;
/// The query will be converted to:
/// select a, b , c , d from t;
pub struct ArithExprSimplification {}

impl PlanRewriter for ArithExprSimplification {
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

impl ArithExprSimplification {
    fn rewrite_expression(&mut self, expr: BoundExpr) -> BoundExpr {
        // TODO: support more data types.
        match &expr.kind {
            BoundExprKind::BinaryOp(binary_op) => match &binary_op.op {
                BinaryOperator::Plus => {
                    match (&binary_op.left_expr.kind, &binary_op.right_expr.kind) {
                        (
                            BoundExprKind::Constant(DataValue::Int32(0)),
                            BoundExprKind::ColumnRef(col),
                        )
                        | (
                            BoundExprKind::ColumnRef(col),
                            BoundExprKind::Constant(DataValue::Int32(0)),
                        ) => BoundExpr {
                            kind: BoundExprKind::ColumnRef(col.clone()),
                            return_type: Some(col.desc.datatype().clone()),
                        },
                        (
                            BoundExprKind::Constant(DataValue::Float64(val)),
                            BoundExprKind::ColumnRef(col),
                        )
                        | (
                            BoundExprKind::ColumnRef(col),
                            BoundExprKind::Constant(DataValue::Float64(val)),
                        ) => {
                            if *val == 0.0 {
                                BoundExpr {
                                    kind: BoundExprKind::ColumnRef(col.clone()),
                                    return_type: Some(col.desc.datatype().clone()),
                                }
                            } else {
                                expr
                            }
                        }
                        _ => expr,
                    }
                }
                BinaryOperator::Minus => {
                    match (&binary_op.left_expr.kind, &binary_op.right_expr.kind) {
                        (
                            BoundExprKind::ColumnRef(col),
                            BoundExprKind::Constant(DataValue::Int32(0)),
                        ) => BoundExpr {
                            kind: BoundExprKind::ColumnRef(col.clone()),
                            return_type: Some(col.desc.datatype().clone()),
                        },
                        (
                            BoundExprKind::ColumnRef(col),
                            BoundExprKind::Constant(DataValue::Float64(val)),
                        ) => {
                            if *val == 0.0 {
                                BoundExpr {
                                    kind: BoundExprKind::ColumnRef(col.clone()),
                                    return_type: Some(col.desc.datatype().clone()),
                                }
                            } else {
                                expr
                            }
                        }
                        _ => expr,
                    }
                }
                BinaryOperator::Multiply => {
                    match (&binary_op.left_expr.kind, &binary_op.right_expr.kind) {
                        (
                            BoundExprKind::Constant(DataValue::Int32(1)),
                            BoundExprKind::ColumnRef(col),
                        )
                        | (
                            BoundExprKind::ColumnRef(col),
                            BoundExprKind::Constant(DataValue::Int32(1)),
                        ) => BoundExpr {
                            kind: BoundExprKind::ColumnRef(col.clone()),
                            return_type: Some(col.desc.datatype().clone()),
                        },
                        (
                            BoundExprKind::Constant(DataValue::Float64(val)),
                            BoundExprKind::ColumnRef(col),
                        )
                        | (
                            BoundExprKind::ColumnRef(col),
                            BoundExprKind::Constant(DataValue::Float64(val)),
                        ) => {
                            if *val == 1.0 {
                                BoundExpr {
                                    kind: BoundExprKind::ColumnRef(col.clone()),
                                    return_type: Some(col.desc.datatype().clone()),
                                }
                            } else {
                                expr
                            }
                        }
                        _ => expr,
                    }
                }
                BinaryOperator::Divide => {
                    match (&binary_op.left_expr.kind, &binary_op.right_expr.kind) {
                        (
                            BoundExprKind::ColumnRef(col),
                            BoundExprKind::Constant(DataValue::Int32(1)),
                        ) => BoundExpr {
                            kind: BoundExprKind::ColumnRef(col.clone()),
                            return_type: Some(col.desc.datatype().clone()),
                        },
                        (
                            BoundExprKind::ColumnRef(col),
                            BoundExprKind::Constant(DataValue::Float64(val)),
                        ) => {
                            if *val == 1.0 {
                                BoundExpr {
                                    kind: BoundExprKind::ColumnRef(col.clone()),
                                    return_type: Some(col.desc.datatype().clone()),
                                }
                            } else {
                                expr
                            }
                        }
                        _ => expr,
                    }
                }
                _ => expr,
            },
            _ => expr,
        }
    }
}
