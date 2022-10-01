// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::binder::BoundExpr;
use crate::binder::BoundExpr::*;
use crate::parser::BinaryOperator::*;
use crate::parser::UnaryOperator;
use crate::types::DataTypeKind as Ty;
use crate::types::DataValue::*;

/// Arithemtic expression simplification rule prunes the useless constant in the binary expressions.
///
/// For example,
/// `select 1 * a, b / 1, c + 0, d - 0 from t;`
/// The query will be converted to:
/// `select a, b, c, d from t;`
pub struct ArithExprSimplificationRule;

impl ExprRewriter for ArithExprSimplificationRule {
    // TODO: support more data types.

    fn rewrite_binary_op(&self, expr: &mut BoundExpr) {
        let new = match &expr {
            BinaryOp(op) => match (&op.op, &*op.left_expr, &*op.right_expr) {
                // x + 0, 0 + x
                (Plus, Constant(Int32(0)), other) => other.clone(),
                (Plus, other, Constant(Int32(0))) => other.clone(),
                (Plus, Constant(Float64(f)), other) if *f == 0.0 => other.clone(),
                (Plus, other, Constant(Float64(f))) if *f == 0.0 => other.clone(),
                // x - 0
                (Minus, other, Constant(Int32(0))) => other.clone(),
                (Minus, other, Constant(Float64(f))) if *f == 0.0 => other.clone(),
                // x * 0, 0 * x
                (Multiply, Constant(Int32(0)), _) => Constant(Int32(0)),
                (Multiply, _, Constant(Int32(0))) => Constant(Int32(0)),
                (Multiply, Constant(Float64(f)), _) if *f == 0.0 => Constant(Float64(0.0.into())),
                (Multiply, _, Constant(Float64(f))) if *f == 0.0 => Constant(Float64(0.0.into())),
                // x * 1, 1 * x
                (Multiply, Constant(Int32(1)), other) => other.clone(),
                (Multiply, other, Constant(Int32(1))) => other.clone(),
                (Multiply, Constant(Float64(f)), other) if *f == 1.0 => other.clone(),
                (Multiply, other, Constant(Float64(f))) if *f == 1.0 => other.clone(),
                // x / 1
                (Divide, other, Constant(Int32(1))) => other.clone(),
                (Divide, other, Constant(Float64(f))) if *f == 1.0 => other.clone(),
                _ => return,
            },
            _ => unreachable!(),
        };
        *expr = new;
    }

    fn rewrite_unary_op(&self, expr: &mut BoundExpr) {
        let new = match &expr {
            UnaryOp(op) => match (&op.op, &*op.expr) {
                (UnaryOperator::Plus, other) => other.clone(),
                _ => return,
            },
            _ => unreachable!(),
        };
        *expr = new;
    }

    fn rewrite_type_cast(&self, expr: &mut BoundExpr) {
        let new = match &expr {
            TypeCast(op) => match (&op.ty, &*op.expr) {
                (Ty::Boolean, k @ Constant(Bool(_))) => k.clone(),
                (Ty::Int(_), k @ Constant(Int32(_))) => k.clone(),
                (Ty::BigInt(_), k @ Constant(Int64(_))) => k.clone(),
                (Ty::Double, k @ Constant(Float64(_))) => k.clone(),
                (Ty::String, k @ Constant(String(_))) => k.clone(),
                _ => return,
            },
            _ => unreachable!(),
        };
        *expr = new;
    }
}

impl PlanRewriter for ArithExprSimplificationRule {
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

#[cfg(test)]
mod tests {
    use std::fs;

    use manifest_dir_macros;
    use serde::Serialize;

    use crate::Database;

    #[tokio::main]
    #[test]
    async fn test_expr_simplification() {
        let db = Database::new_in_memory();
        let create_stmt = "create table t(a int)";
        let sql0 = "select a + 0 from t";
        // a + 0 should be converted to a
        let _ = db.run(create_stmt).await;

        let plans = db.generate_execution_plan(sql0).unwrap();
        assert_eq!(plans.len(), 1);
        let buf = Vec::new();
        let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
        let mut ser = serde_json::Serializer::with_formatter(buf, formatter);
        plans[0].serialize(&mut ser).unwrap();
        let ser_str = String::from_utf8(ser.into_inner()).unwrap();

        let data = fs::read_to_string(manifest_dir_macros::file_relative_path!(
            "tests/json/arith_expr_simplification.json"
        ))
        .unwrap();

        assert_eq!(ser_str, data);
    }
}
