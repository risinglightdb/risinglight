//! Analyze schema and replace all column references with physical indices.
//!
//! This is the final step before executing.

use super::*;
use crate::types::ColumnIndex;

/// Replaces all column references (`ColumnRefId`) with
/// physical indices ([`ColumnIndex`]) to the given schema.
///
/// # Example
/// - given schema:           `sum(v1), v2`
/// - the expressions:        `v2 + 1, sum(v1) + v2`
/// - should be rewritten to: `#1 + 1, #0 + #1`
pub fn resolve_column_index(expr: RecExpr, schema: &[RecExpr]) -> RecExpr {
    let mut egraph = egg::EGraph::<Expr, ()>::default();
    // add expressions from schema and union them with index
    for (i, expr) in schema.iter().enumerate() {
        let id1 = egraph.add_expr(expr);
        let id2 = egraph.add(Expr::ColumnIndex(ColumnIndex(i as u32)));
        egraph.union(id1, id2);
    }
    // define cost function
    struct PreferColumnIndex;
    impl CostFunction<Expr> for PreferColumnIndex {
        type Cost = u32;
        fn cost<C>(&mut self, enode: &Expr, mut costs: C) -> Self::Cost
        where
            C: FnMut(Id) -> Self::Cost,
        {
            let op_cost = match enode {
                Expr::Column(_) => u32::MAX, // column ref should no longer exists
                _ => 1,
            };
            enode.fold(op_cost, |sum, id| {
                sum.checked_add(costs(id)).unwrap_or(u32::MAX)
            })
        }
    }
    // extract the best expression
    let id = egraph.add_expr(&expr);
    let extractor = egg::Extractor::new(&egraph, PreferColumnIndex);
    let (_, best) = extractor.find_best(id);
    best
}

/// The data type of schema analysis.
pub type Schema = Option<Vec<Id>>;

/// Returns the output expressions for plan node.
pub fn analyze_schema(egraph: &EGraph, enode: &Expr) -> Schema {
    use Expr::*;
    let x = |i: Id| egraph[i].data.schema.clone();
    let concat = |v1: Vec<Id>, v2: Vec<Id>| v1.into_iter().chain(v2.into_iter()).collect();
    Some(match enode {
        // equal to child
        Filter([_, c]) | Order([_, c]) | Limit([_, _, c]) | TopN([_, _, _, c]) => x(*c)?,

        // concat 2 children
        Join([_, _, l, r]) | HashJoin([_, _, _, l, r]) => concat(x(*l)?, x(*r)?),

        // list is the source for the following nodes
        List(ids) => ids.to_vec(),

        // plans that change schema
        Scan(columns) => x(*columns)?,
        Values(_) => todo!("add schema for values plan"),
        Proj([exprs, _]) | Select([exprs, ..]) => x(*exprs)?,
        Agg([exprs, group_keys, _]) => concat(x(*exprs)?, x(*group_keys)?),

        // prune node may changes the schema, but we don't know the exact result for now
        // so just return `None` to indicate "unknown"
        Prune(_) => return None,

        // not plan node
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::resolve_column_index;

    macro_rules! test_resolve_column_index {
        ($name:ident,rewrite: $input:expr,schema: $schema:expr,expect: $expected:expr,) => {
            #[test]
            fn $name() {
                let input = $input.parse().unwrap();
                let schema = $schema.iter().map(|s| s.parse().unwrap()).collect_vec();
                let actual = resolve_column_index(input, &schema);
                assert_eq!(actual.to_string(), $expected);
            }
        };
    }

    test_resolve_column_index!(
        resolve_column_index1,
        rewrite: "(list (+ (+ $1.2 1) (sum $1.1)))",
        schema: ["(+ $1.2 1)", "(sum $1.1)", "$1.2"],
        expect: "(list (+ #0 #1))",
    );
}
