// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

//! Analyze whether the table is ordered by some keys.

use super::*;

/// The data type of order analysis.
///
/// If the table is unordered, the value is `None`.
/// Otherwise, the value is an Id of order key node. e.g. `(list (asc a) (desc b))`
pub type OrderKey = Option<Id>;

/// Returns the order key for plan node.
pub fn analyze_order(enode: &Expr, x: impl Fn(&Id) -> OrderKey) -> OrderKey {
    use Expr::*;
    match enode {
        // TODO: scanned table is ordered by primary key in secondary storage
        Scan([_, _columns]) => None,
        // plans that sort rows
        Order([keys, _]) | TopN([_, _, keys, _]) => Some(*keys),
        // plans that preserve order
        Proj([_, c]) | Filter([_, c]) | Window([_, c]) | Limit([_, _, c]) => x(c),
        // check order
        MergeJoin([_, _, _, l, r]) => {
            let (lkey, rkey) = (x(l), x(r));
            assert!(
                lkey.is_some() && rkey.is_some(),
                "children of merge join should be ordered"
            );
            // assert_eq!(lkey, rkey, "children of merge join should have same order");
            lkey
        }
        SortAgg([_, _groupby, c]) => {
            // TODO: check if c is ordered by groupby
            x(c)
        }
        // unordered for other plans
        _ => None,
    }
}

#[rustfmt::skip]
pub fn order_rules() -> Vec<Rewrite> { vec![
    rw!("useless-order";
        "(order ?keys ?child)" => "?child"
        if is_orderby("?keys", "?child")
    ),
    rw!("merge-join";
        "(hashjoin ?type (list ?kl) (list ?kr) ?left ?right)" =>
        "(mergejoin ?type (list ?kl) (list ?kr)
            (order (list (asc ?kl)) ?left)
            (order (list (asc ?kr)) ?right)
        )"
    ),
    rw!("sort-agg";
        "(agg ?aggs (list ?k) ?child)" =>
        "(sortagg ?aggs (list ?k)
            (order (list (asc ?k)) ?child))"
    ),
]}

/// Returns true if the plan is ordered by the keys.
fn is_orderby(keys: &str, plan: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let keys = var(keys);
    let plan = var(plan);
    move |egraph, _, subst| {
        let keys = subst[keys];
        let plan_keys = egraph[subst[plan]].data.orderby;
        plan_keys == Some(keys)
    }
}
