use super::*;
use crate::binder::{BoundBinaryOp, BoundExpr, BoundExprKind::*};
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

impl PlanRewriter for BoolExprSimplification {
    fn rewrite_filter(&mut self, plan: &LogicalFilter) -> Option<LogicalPlanRef> {
        let new_expr = self.rewrite_expr(plan.expr.clone());
        match &new_expr.kind {
            Constant(Bool(false) | Null) => Some(
                LogicalPlan::LogicalFilter(LogicalFilter {
                    expr: new_expr,
                    child: (LogicalPlan::Dummy.into()),
                })
                .into(),
            ),
            Constant(Bool(true)) => Some(self.rewrite_plan(plan.get_child())),
            _ => Some(
                LogicalPlan::LogicalFilter(LogicalFilter {
                    expr: new_expr,
                    child: self.rewrite_plan(plan.get_child()),
                })
                .into(),
            ),
        }
    }

    fn rewrite_expr(&mut self, expr: BoundExpr) -> BoundExpr {
        let new_kind = match expr.kind {
            BinaryOp(op) => {
                let left = self.rewrite_expr(*op.left_expr);
                let right = self.rewrite_expr(*op.right_expr);
                match (op.op, &left.kind, &right.kind) {
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
                    (op, _, _) => BinaryOp(BoundBinaryOp {
                        left_expr: Box::new(left),
                        op,
                        right_expr: Box::new(right),
                    }),
                }
            }
            // FIXME: rewrite child expressions
            _ => expr.kind,
        };
        BoundExpr {
            kind: new_kind,
            return_type: expr.return_type,
        }
    }
}
