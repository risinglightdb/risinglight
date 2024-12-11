// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! Analyze the output schema of plans.

use super::*;

/// The data type of schema analysis.
pub type Schema = Vec<Id>;

/// Returns the output expressions for plan node.
pub fn analyze_schema(
    enode: &Expr,
    schema: impl Fn(&Id) -> Schema,
    node0: impl Fn(&Id) -> Expr,
) -> Schema {
    use Expr::*;
    let concat = |v1: Vec<Id>, v2: Vec<Id>| {
        assert!(
            v2.iter().all(|id| !v1.contains(id)),
            "conflict columns for node: {enode:?}"
        );
        v1.into_iter().chain(v2).collect()
    };
    match enode {
        // equal to child
        Filter([_, c]) | Order([_, c]) | Limit([_, _, c]) | TopN([_, _, _, c]) | Empty(c) => {
            schema(c)
        }

        // concat 2 children
        Join([t, _, l, r])
        | HashJoin([t, _, _, _, l, r])
        | MergeJoin([t, _, _, _, l, r])
        | Apply([t, l, r]) => match node0(t) {
            Semi | Anti => schema(l),
            Mark => concat(schema(l), vec![*t]), // child || Mark
            _ => concat(schema(l), schema(r)),
        },

        // list is the source for the following nodes
        List(ids) => ids.to_vec(),

        // plans that change schema
        Scan([_, columns, _]) => schema(columns),
        Values(vs) => schema(&vs[0]),
        Proj([exprs, _]) | Agg([exprs, _]) => schema(exprs),
        Window([exprs, child]) => concat(schema(child), schema(exprs)),
        HashAgg([keys, aggs, _]) | SortAgg([keys, aggs, _]) => concat(schema(keys), schema(aggs)),

        // not plan node
        _ => vec![],
    }
}
