use super::*;
use crate::binder::BoundExprKind;
use crate::binder::{BoundExpr, BoundExprKind::*};
use crate::parser::BinaryOperator::*;
use crate::types::DataValue;
/// Boolean expression simplification rule will rewrite expression which compares ('>=', '<' and
/// '=') with null. (You need `a is null`!)
/// Moroever, when the filtering condition is always false, we will prune the child logical plan,
/// when the filtering condition is always true, we will prune logical filter plan.
/// For example:
/// `select * from t where a == null`
/// The query will be converted to:
/// `select '';`
pub struct BoolExprSimplification;

impl PlanRewriter for BoolExprSimplification {
    fn rewrite_expr(&mut self, expr: BoundExpr) -> BoundExpr {
        self.rewrite_null_expr(expr)
    }

    fn rewrite_filter(&mut self, plan: LogicalFilter) -> LogicalPlan {
        let new_expr = self.rewrite_expr(plan.expr);
        match &new_expr.kind {
            BoundExprKind::Constant(DataValue::Bool(false)) => LogicalPlan::Filter(LogicalFilter {
                expr: new_expr,
                child: Box::new(LogicalPlan::Dummy),
            }),
            BoundExprKind::Constant(DataValue::Bool(true)) => self.rewrite_plan(*plan.child),
            _ => LogicalPlan::Filter(LogicalFilter {
                expr: new_expr,
                child: Box::new(self.rewrite_plan(*plan.child)),
            }),
        }
    }
}

impl BoolExprSimplification {
    pub fn rewrite_null_expr(&mut self, expr: BoundExpr) -> BoundExpr {
        let new_kind = match &expr.kind {
            BinaryOp(op) => match (&op.op, &op.left_expr.kind, &op.right_expr.kind) {
                (Eq, Constant(DataValue::Null), _)
                | (Eq, _, Constant(DataValue::Null))
                | (NotEq, Constant(DataValue::Null), _)
                | (NotEq, _, Constant(DataValue::Null))
                | (Gt, _, Constant(DataValue::Null))
                | (Gt, Constant(DataValue::Null), _)
                | (Lt, Constant(DataValue::Null), _)
                | (Lt, _, Constant(DataValue::Null))
                | (GtEq, Constant(DataValue::Null), _)
                | (GtEq, _, Constant(DataValue::Null))
                | (LtEq, _, Constant(DataValue::Null))
                | (LtEq, Constant(DataValue::Null), _) => {
                    BoundExprKind::Constant(DataValue::Bool(false))
                }
                _ => expr.kind.clone(),
            },
            _ => expr.kind.clone(),
        };
        BoundExpr {
            kind: new_kind,
            return_type: expr.return_type,
        }
    }
}
