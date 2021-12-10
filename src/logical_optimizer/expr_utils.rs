use crate::logical_optimizer::BoundExpr;
use crate::logical_optimizer::BoundExpr::BinaryOp;
use crate::parser::BinaryOperator::And;
use bit_set::BitSet;
pub fn conjunctions_inner(expr: BoundExpr, rets: &mut Vec<BoundExpr>) {
    if matches!(expr, BinaryOp(ref binary_expr) if binary_expr.op == And) {
        if let BinaryOp(bin_expr) = expr {
            conjunctions_inner(*bin_expr.left_expr, rets);
            conjunctions_inner(*bin_expr.right_expr, rets);
        } else {
            unreachable!()
        }
    } else {
        rets.push(expr);
    }
}

pub fn conjunctions(expr: BoundExpr) -> Vec<BoundExpr> {
    let mut rets = vec![];
    conjunctions_inner(expr, &mut rets);
    rets
}

#[allow(dead_code)]
pub fn to_cnf(expr: BoundExpr) -> Vec<BoundExpr> {
    // FIXME：TODO it is just convering to conjunctions now
    conjunctions(expr)
}

#[allow(dead_code)]
pub fn input_col_refs(expr: &BoundExpr) -> BitSet {
    let mut set = BitSet::default();
    input_col_refs_inner(expr, &mut set);
    set
}

pub fn input_col_refs_inner(expr: &BoundExpr, input_set: &mut BitSet) {
    use BoundExpr::*;

    match expr {
        ColumnRef(_) => {
            unreachable!()
        }
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
        _ => unreachable!(),
    };
}
