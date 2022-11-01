use super::*;

pub type Rows = u32;

/// Returns the estimate rows.
pub fn analyze_rows(egraph: &EGraph, enode: &Expr) -> Rows {
    use Expr::*;
    let x = |i: &Id| egraph[*i].data.rows.clone();
    let get_limit_num = |id: &Id| {
        (egraph[*id].data.constant.as_ref())
            .expect("limit should be constant")
            .as_usize()
            .unwrap()
            .map_or(u32::MAX, |x| x as u32)
    };
    match enode {
        Values(v) => v.len() as u32,
        Scan(_) => 100, // TODO: get from table
        Proj([_, c]) | Order([_, c]) => x(c),
        Agg([_, _, c]) => 1,                        // TODO: group by cardinality
        Filter([_, c]) => (x(c) as f32 * 0.8) as _, // TODO: estimate predicate
        Limit([_, limit, c]) | TopN([_, limit, _, c]) => x(c).min(get_limit_num(limit)),
        Join([_, _, l, r]) => x(l).checked_mul(x(r)).unwrap_or(u32::MAX),
        HashJoin([_, _, _, l, r]) => x(l) + x(r),
        _ => 1,
    }
}
