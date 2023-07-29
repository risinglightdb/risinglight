// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::types::DataValue;

/// The data type of row number analysis.
pub type Rows = f32;

/// Returns the estimated rows for plans, or selectivity for expressions.
pub fn analyze_rows(egraph: &EGraph, enode: &Expr) -> Rows {
    use Expr::*;
    let x = |i: &Id| egraph[*i].data.rows;
    let get_limit_num = |id: &Id| {
        (egraph[*id].data.constant.as_ref())
            .expect("limit should be constant")
            .as_usize()
            .unwrap()
            .map_or(f32::MAX, |x| x as f32)
    };
    let list_len = |id: &Id| egraph[*id].as_list().len();
    match enode {
        // for plan nodes, the result represents estimated rows
        Values(v) => v.len() as f32,
        Scan(_) => 1000.0, // TODO: get from table
        Proj([_, c]) | Order([_, c]) | Window([_, c]) => x(c),
        Agg(_) => 1.0,
        HashAgg([_, _, c]) | SortAgg([_, _, c]) => {
            x(c) / 2.0 // TODO: group by cardinality
        }
        Filter([cond, c]) => x(c) * x(cond),
        Limit([limit, _, c]) | TopN([limit, _, _, c]) => x(c).min(get_limit_num(limit)),
        Join([_, on, l, r]) => x(l) * x(r) * x(on),
        HashJoin([_, _, _, l, r]) | MergeJoin([_, _, _, l, r]) => x(l).max(x(r)),
        Empty(_) => 0.0,

        // for expressions, the result represents selectivity
        Ref(a) => x(a),
        Constant(DataValue::Bool(false)) => 0.0,
        Constant(DataValue::Bool(true)) => 1.0,
        And([a, b]) => x(a) * x(b), // TODO: consider dependency
        Or([a, b]) => x(a) + x(b) - x(a) * x(b), // TODO: consider dependency
        Not(a) => 1.0 - x(a),
        Gt(_) | Lt(_) | GtEq(_) | LtEq(_) | Eq(_) | NotEq(_) => 0.5,
        In([_, b]) => 1.0 - 1.0 / (list_len(b) as f32 + 1.0),

        _ => 1.0,
    }
}
