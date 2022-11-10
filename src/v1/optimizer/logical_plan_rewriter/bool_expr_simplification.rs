// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::parser::BinaryOperator::*;
use crate::types::DataValue::*;
use crate::v1::binder::BoundExpr;
use crate::v1::binder::BoundExpr::*;
use crate::v1::optimizer::plan_nodes::Dummy;

/// Boolean expression simplification rule will rewrite expression which compares ('>=', '<' and
/// '=') with null. (You need `a is null`!)
///
/// Moreover, when the filtering condition is always false, we will prune the child logical plan,
/// when the filtering condition is always true, we will prune logical filter plan.
///
/// For example:
///
/// - `select * from t where a == null` => `select ''`
/// - `select * from t where 1 == 1` => `select * from t`
pub struct BoolExprSimplificationRule;

impl ExprRewriter for BoolExprSimplificationRule {
    fn rewrite_binary_op(&self, expr: &mut BoundExpr) {
        let new = match expr {
            BinaryOp(op) => {
                self.rewrite_expr(&mut op.left_expr);
                self.rewrite_expr(&mut op.right_expr);
                match (&op.op, &*op.left_expr, &*op.right_expr) {
                    (And, Constant(Bool(false)), _) => Constant(Bool(false)),
                    (And, _, Constant(Bool(false))) => Constant(Bool(false)),
                    (And, Constant(Bool(true)), other) => other.clone(),
                    (And, other, Constant(Bool(true))) => other.clone(),
                    (Or, Constant(Bool(true)), _) => Constant(Bool(true)),
                    (Or, _, Constant(Bool(true))) => Constant(Bool(true)),
                    (Or, Constant(Bool(false)), other) => other.clone(),
                    (Or, other, Constant(Bool(false))) => other.clone(),
                    (Eq | NotEq | Gt | Lt | GtEq | LtEq, Constant(Null), _) => Constant(Null),
                    (Eq | NotEq | Gt | Lt | GtEq | LtEq, _, Constant(Null)) => Constant(Null),
                    _ => BinaryOp(op.clone()),
                }
            }
            _ => unreachable!(),
        };
        *expr = new;
    }
}

impl PlanRewriter for BoolExprSimplificationRule {
    fn rewrite_logical_filter(&mut self, plan: &LogicalFilter) -> PlanRef {
        let child = self.rewrite(plan.child());
        let new_plan = Arc::new(plan.clone_with_rewrite_expr(child, self));
        match &new_plan.expr() {
            Constant(Bool(false) | Null) => Arc::new(Dummy::new(new_plan.schema())),
            Constant(Bool(true)) => return plan.child().clone(),
            _ => new_plan,
        }
    }
}
