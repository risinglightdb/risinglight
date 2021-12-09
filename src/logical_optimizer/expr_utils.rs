use crate::logical_optimizer::BoundExpr;
use crate::logical_optimizer::BoundExpr::BinaryOp;
use crate::parser::BinaryOperator::And;
pub fn conjunctions_inner(expr: BoundExpr, rets: &mut Vec<BoundExpr>) {
    let mut is_and = false;
    if let BinaryOp(ref bin_expr) = expr {
        if bin_expr.op == And {
            is_and = true;
        }
    }
    if is_and {
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
