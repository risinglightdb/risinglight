use super::*;
use crate::binder::{BoundExpr, BoundExprKind};
use crate::parser::BinaryOperator;
use crate::types::DataValue;

/// Arithemtic expression simplification rule prunes the useless constant in the binary expressions.
///
/// For example,
/// `select 1 * a, b / 1, c + 0, d - 0 from t;`
/// The query will be converted to:
/// `select a, b, c, d from t;`
pub struct ArithExprSimplification;

impl PlanRewriter for ArithExprSimplification {
    fn rewrite_expr(&mut self, expr: BoundExpr) -> BoundExpr {
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
