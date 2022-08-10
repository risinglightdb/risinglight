// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use bit_set::BitSet;

use super::logical_plan_rewriter::{BoolExprSimplificationRule, ExprRewriter};
use crate::binder::BoundBinaryOp;
use crate::optimizer::BoundExpr;
use crate::optimizer::BoundExpr::BinaryOp;
use crate::parser::BinaryOperator::And;
use crate::types::{DataTypeExt, DataTypeKind, DataValue};
pub fn conjunctions_inner(expr: BoundExpr, rets: &mut Vec<BoundExpr>) {
    match expr {
        BinaryOp(bin_expr) if bin_expr.op == And => {
            conjunctions_inner(*bin_expr.left_expr, rets);
            conjunctions_inner(*bin_expr.right_expr, rets);
        }
        _ => rets.push(expr),
    }
}

pub fn conjunctions(expr: BoundExpr) -> Vec<BoundExpr> {
    let mut rets = vec![];
    conjunctions_inner(expr, &mut rets);
    rets
}

pub fn merge_conjunctions<I>(iter: I) -> BoundExpr
where
    I: Iterator<Item = BoundExpr>,
{
    let mut ret = BoundExpr::Constant(DataValue::Bool(true));
    for expr in iter {
        ret = BoundExpr::BinaryOp(BoundBinaryOp {
            op: And,
            left_expr: Box::new(ret),
            right_expr: Box::new(expr),
            return_type: Some(DataTypeKind::Boolean.nullable()),
        })
    }
    let rewriter = BoolExprSimplificationRule {};
    rewriter.rewrite_expr(&mut ret);
    ret
}

pub fn input_col_refs(expr: &BoundExpr) -> BitSet {
    let mut set = BitSet::default();
    input_col_refs_inner(expr, &mut set);
    set
}

pub fn input_col_refs_inner(expr: &BoundExpr, input_set: &mut BitSet) {
    use BoundExpr::*;

    match expr {
        ColumnRef(_) => {}
        InputRef(input_ref) => {
            input_set.insert(input_ref.index);
        }
        AggCall(agg) => {
            for arg in &agg.args {
                input_col_refs_inner(arg, input_set);
            }
        }
        BinaryOp(binary_op) => {
            input_col_refs_inner(binary_op.left_expr.as_ref(), input_set);
            input_col_refs_inner(binary_op.right_expr.as_ref(), input_set);
        }
        UnaryOp(unary_op) => input_col_refs_inner(unary_op.expr.as_ref(), input_set),
        TypeCast(cast) => input_col_refs_inner(cast.expr.as_ref(), input_set),
        IsNull(isnull) => input_col_refs_inner(isnull.expr.as_ref(), input_set),
        ExprWithAlias(inner) => input_col_refs_inner(inner.expr.as_ref(), input_set),
        Constant(_) => {}
        Alias(_) => {}
    };
}

pub fn shift_input_col_refs(expr: &mut BoundExpr, delta: i32) {
    use BoundExpr::*;
    match expr {
        ColumnRef(_) => {}
        InputRef(input_ref) => {
            input_ref.index = (input_ref.index as i32 + delta) as usize;
        }
        AggCall(agg) => {
            for arg in &mut agg.args {
                shift_input_col_refs(&mut *arg, delta);
            }
        }
        BinaryOp(binary_op) => {
            shift_input_col_refs(&mut binary_op.left_expr, delta);
            shift_input_col_refs(&mut binary_op.right_expr, delta);
        }
        UnaryOp(unary_op) => shift_input_col_refs(&mut unary_op.expr, delta),
        TypeCast(cast) => shift_input_col_refs(&mut cast.expr, delta),
        IsNull(isnull) => shift_input_col_refs(&mut isnull.expr, delta),
        ExprWithAlias(inner) => shift_input_col_refs(&mut inner.expr, delta),
        Constant(_) => {}
        Alias(_) => {}
    };
}
