use egg::{Id, PatternAst};

use super::{EGraph, Expr, RecExpr};

fn do_explain(egraph: &EGraph, enode: &Expr) -> Option<String> {
    let x = |i: &Id| egraph[*i].data.constant.as_ref();
    match enode {
        Expr::TopN([limit, offset, order_bys, child]) => Some(format!(
            "TopN {{ limit={}, offset={} }}",
            x(limit)?,
            x(offset)?
        )),
        _ => None,
    }
}

pub fn explain(expr: &RecExpr, width: usize) -> String {
    let mut runner = egg::Runner::default().with_expr(expr);
    assert_eq!(runner.roots.len(), 1);
    do_explain(&runner.egraph, &expr[runner.roots[0]]).unwrap()
}
