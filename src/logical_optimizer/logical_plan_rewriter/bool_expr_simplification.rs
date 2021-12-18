use super::*;
use crate::binder::BoundExpr;
use crate::binder::BoundExpr::*;
use crate::logical_optimizer::plan_nodes::Dummy;
use crate::parser::BinaryOperator::*;
use crate::types::DataValue::*;

/// Boolean expression simplification rule will rewrite expression which compares ('>=', '<' and
/// '=') with null. (You need `a is null`!)
/// Moroever, when the filtering condition is always false, we will prune the child logical plan,
/// when the filtering condition is always true, we will prune logical filter plan.
/// For example:
/// `select * from t where a == null`
/// The query will be converted to:
/// `select '';`
/// `select * from t where 1 == 1`
/// The query will be converted to:
/// `select * from t`
pub struct BoolExprSimplification;

impl Rewriter for BoolExprSimplification {
    fn rewrite_logical_filter(&mut self, mut plan: LogicalFilter) -> PlanRef {
        match &plan.expr {
            Constant(Bool(false) | Null) => plan.child = Rc::new(Dummy {}),
            Constant(Bool(true)) => return plan.child,
            _ => {}
        }
        Rc::new(plan)
    }

    fn rewrite_expr(&mut self, expr: &mut BoundExpr) {
        let new = match expr {
            BinaryOp(op) => {
                self.rewrite_expr(&mut *op.left_expr);
                self.rewrite_expr(&mut *op.right_expr);
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
            // FIXME: rewrite child expressions
            _ => expr.clone(),
        };
        *expr = new;
    }
}
