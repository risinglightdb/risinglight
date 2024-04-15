// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! Analyze whether the table is ordered by some keys.

use super::*;

/// The data type of order analysis.
///
/// The value is a list of `Id`s to order key nodes. e.g. `[a, (desc b)]`.
/// If unordered, the value is `[]`.
pub type OrderKey = Box<[Id]>;

/// Returns the order key for plan node.
pub fn analyze_order(egraph: &EGraph, enode: &Expr) -> OrderKey {
    use Expr::*;
    let x = |id: &Id| &egraph[*id].data.orderby;
    match enode {
        List(keys) => keys.clone(),
        // scanned table is ordered by primary key in secondary storage
        Scan([_, cols, _]) if egraph.analysis.config.table_is_sorted_by_primary_key => {
            let primary_key = egraph[*cols].as_list().iter().find(|id| {
                let catalog = &egraph.analysis.catalog;
                match catalog.get_column(&egraph[**id].as_column()) {
                    Some(col) => col.is_primary(),
                    None => false,
                }
            });
            match primary_key {
                Some(id) => Box::new([*id]),
                None => Box::new([]),
            }
        }
        // plans that sort rows
        Order([keys, _]) | TopN([_, _, keys, _]) => x(keys).clone(),
        // plans that preserve order
        Proj([_, c]) | Filter([_, c]) | Window([_, c]) | Limit([_, _, c]) => x(c).clone(),
        MergeJoin([_, _, _, _, _, r]) => x(r).clone(),
        SortAgg([_, _, c]) => x(c).clone(),
        // unordered for other plans
        _ => Box::new([]),
    }
}

#[rustfmt::skip]
pub fn order_rules() -> Vec<Rewrite> { vec![
    rw!("useless-order";
        "(order ?keys ?child)" => "?child"
        if is_orderby("?keys", "?child")
    ),
    rw!("merge-join";
        "(hashjoin ?type ?cond ?lkey ?rkey ?left ?right)" =>
        "(mergejoin ?type ?cond ?lkey ?rkey ?left ?right)"
        if is_orderby("?lkey", "?left")
        if is_orderby("?rkey", "?right")
    ),
    rw!("sort-agg";
        "(hashagg ?keys ?aggs ?child)" =>
        "(sortagg ?keys ?aggs ?child)"
        if is_orderby("?keys", "?child")
    ),
]}

/// Returns true if the plan is ordered by the keys.
fn is_orderby(keys: &str, plan: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let keys = var(keys);
    let plan = var(plan);
    move |egraph, _, subst| {
        let keys = &egraph[subst[keys]].data.orderby;
        let plan_keys = &egraph[subst[plan]].data.orderby;
        plan_keys.starts_with(keys)
    }
}
