// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;

/// The data type of aggragation analysis.
pub type AggSet = Vec<Expr>;

/// Returns all aggragations in the tree.
///
/// Note: if there is an agg over agg, e.g. `sum(count(a))`, only the upper one will be returned.
pub fn analyze_aggs(enode: &Expr, x: impl Fn(&Id) -> AggSet) -> AggSet {
    if enode.is_aggregate_function() {
        return vec![enode.clone()];
    }
    if let Expr::Over(_) = enode {
        return vec![];
    }
    // merge the set from all children
    // TODO: ignore plan nodes
    enode.children().iter().flat_map(x).collect()
}

/// The data type of over analysis.
pub type OverSet = Vec<Expr>;

/// Returns all over nodes in the tree.
pub fn analyze_overs(enode: &Expr, x: impl Fn(&Id) -> OverSet) -> OverSet {
    if let Expr::Over(_) = enode {
        return vec![enode.clone()];
    }
    // merge the set from all children
    // TODO: ignore plan nodes
    enode.children().iter().flat_map(x).collect()
}
