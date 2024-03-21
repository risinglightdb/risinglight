// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;

/// The data type of aggragation analysis.
pub type AggSet = Vec<Expr>;

/// Returns all aggragations in the tree.
///
/// Note: if there is an agg over agg, e.g. `sum(count(a))`, only the upper one will be returned.
pub fn analyze_aggs(enode: &Expr, x: impl Fn(&Id) -> AggSet) -> AggSet {
    use Expr::*;
    match enode {
        _ if enode.is_aggregate_function() => vec![enode.clone()],
        Over(_) | Ref(_) | Max1Row(_) => vec![],
        In([a, _]) => x(a),
        // merge the set from all children
        _ => enode.children().iter().flat_map(x).collect(),
    }
}

/// The data type of over analysis.
pub type OverSet = Vec<Expr>;

/// Returns all over nodes in the tree.
pub fn analyze_overs(enode: &Expr, x: impl Fn(&Id) -> OverSet) -> OverSet {
    use Expr::*;
    match enode {
        Over(_) => vec![enode.clone()],
        Ref(_) => vec![],
        // merge the set from all children
        _ => enode.children().iter().flat_map(x).collect(),
    }
}
