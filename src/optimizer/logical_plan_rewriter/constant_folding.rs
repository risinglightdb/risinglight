// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use BoundExpr::*;

use super::*;
use crate::array::ArrayImpl;
use crate::binder::BoundExpr;

/// Constant folding rule aims to evalute the constant expression before query execution.
///
/// For example,
/// `select 3 * 2 * a from t where a >= 100 * 30;`
/// The rule will convert it into
/// `select 6 * a from t where a >= 3000;`
#[derive(Default)]
pub struct ConstantFoldingRule;

impl ExprRewriter for ConstantFoldingRule {
    fn rewrite_binary_op(&self, expr: &mut BoundExpr) {
        match expr {
            BinaryOp(op) => {
                self.rewrite_expr(&mut op.left_expr);
                self.rewrite_expr(&mut op.right_expr);
                if let (Constant(v1), Constant(v2)) = (&*op.left_expr, &*op.right_expr) {
                    let res = ArrayImpl::from(v1)
                        .binary_op(&op.op, &ArrayImpl::from(v2))
                        .get(0);
                    *expr = Constant(res);
                }
            }
            _ => unreachable!(),
        }
    }

    fn rewrite_unary_op(&self, expr: &mut BoundExpr) {
        match expr {
            UnaryOp(op) => {
                self.rewrite_expr(&mut op.expr);
                if let Constant(v) = &*op.expr {
                    let res = ArrayImpl::from(v).unary_op(&op.op).get(0);
                    *expr = Constant(res);
                }
            }
            _ => unreachable!(),
        }
    }

    fn rewrite_type_cast(&self, expr: &mut BoundExpr) {
        match expr {
            TypeCast(cast) => {
                self.rewrite_expr(&mut cast.expr);
                if let Constant(v) = &*cast.expr {
                    if let Ok(array) = ArrayImpl::from(v).try_cast(cast.ty.clone()) {
                        let res = array.get(0);
                        *expr = Constant(res);
                    }
                    // ignore if cast failed
                    // TODO: raise an error
                }
            }
            _ => unreachable!(),
        }
    }
}

impl PlanRewriter for ConstantFoldingRule {
    fn rewrite_logical_join(&mut self, join: &LogicalJoin) -> PlanRef {
        let left = self.rewrite(join.left());
        let right = self.rewrite(join.right());
        Arc::new(join.clone_with_rewrite_expr(left, right, self))
    }

    fn rewrite_logical_projection(&mut self, proj: &LogicalProjection) -> PlanRef {
        let new_child = self.rewrite(proj.child());
        Arc::new(proj.clone_with_rewrite_expr(new_child, self))
    }

    fn rewrite_logical_aggregate(&mut self, agg: &LogicalAggregate) -> PlanRef {
        let new_child = self.rewrite(agg.child());
        Arc::new(agg.clone_with_rewrite_expr(new_child, self))
    }
    fn rewrite_logical_filter(&mut self, plan: &LogicalFilter) -> PlanRef {
        let child = self.rewrite(plan.child());
        Arc::new(plan.clone_with_rewrite_expr(child, self))
    }
    fn rewrite_logical_order(&mut self, plan: &LogicalOrder) -> PlanRef {
        let child = self.rewrite(plan.child());
        Arc::new(plan.clone_with_rewrite_expr(child, self))
    }
    fn rewrite_logical_values(&mut self, plan: &LogicalValues) -> PlanRef {
        Arc::new(plan.clone_with_rewrite_expr(self))
    }
}
