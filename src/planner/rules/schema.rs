//! Analyze schema and replace all column references with physical indices.
//!
//! This is the final step before executing.

use super::*;
use crate::types::ColumnIndex;

#[rustfmt::skip]
pub fn rules() -> Vec<Rewrite> { vec![
    rw!("remove-identity-projection"; 
        "(proj ?expr ?child)" => "?child"
        if schema_is_eq("?expr", "?child")
    ),
]}

/// Replaces all column references (`ColumnRefId`) with
/// physical indices ([`ColumnIndex`]) to the given schema.
///
/// # Example
/// - given schema:           `sum(v1), v2`
/// - the expressions:        `v2 + 1, sum(v1) + v2`
/// - should be rewritten to: `#1 + 1, #0 + #1`
///
/// ```
/// # use risinglight::planner::{RecExpr, ColumnIndexResolver};
/// let schema = "(list (sum v1) v2)".parse().unwrap();
/// let expr = "(list (+ v2 1) (+ (sum v1) v2))".parse().unwrap();
/// assert_eq!(
///     ColumnIndexResolver::new(&schema).resolve(&expr).to_string(),
///     "(list (+ #1 1) (+ #0 #1))"
/// );
/// ```
pub struct ColumnIndexResolver {
    egraph: egg::EGraph<Expr, ()>,
}

impl ColumnIndexResolver {
    pub fn new(schema: &RecExpr) -> Self {
        let mut egraph = egg::EGraph::<Expr, ()>::default();
        let root = egraph.add_expr(schema);
        let list = egraph[root].nodes[0].as_list().to_vec();
        // add expressions from schema and union them with index
        for (i, expr) in list.into_iter().enumerate() {
            let idx = egraph.add(Expr::ColumnIndex(ColumnIndex(i as u32)));
            egraph.union(idx, expr);
        }
        egraph.rebuild();
        ColumnIndexResolver { egraph }
    }

    /// Replaces all column references (`ColumnRefId`) with
    /// physical indices ([`ColumnIndex`]) in the expr.
    pub fn resolve(&mut self, expr: &RecExpr) -> RecExpr {
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
        let id = self.egraph.add_expr(expr);
        let extractor = egg::Extractor::new(&self.egraph, PreferColumnIndex);
        let (_, best) = extractor.find_best(id);
        best
    }
}

/// The data type of schema analysis.
pub type Schema = Option<Vec<Id>>;

/// Returns the output expressions for plan node.
pub fn analyze_schema(enode: &Expr, x: impl Fn(&Id) -> Schema) -> Schema {
    use Expr::*;
    let concat = |v1: Vec<Id>, v2: Vec<Id>| v1.into_iter().chain(v2.into_iter()).collect();
    Some(match enode {
        // equal to child
        Filter([_, c]) | Order([_, c]) | Limit([_, _, c]) | TopN([_, _, _, c]) => x(c)?,

        // concat 2 children
        Join([_, _, l, r]) | HashJoin([_, _, _, l, r]) => concat(x(l)?, x(r)?),

        // list is the source for the following nodes
        List(ids) => ids.to_vec(),

        // plans that change schema
        Scan([_, columns]) => x(columns)?,
        Values(vs) => vs.first().and_then(x)?,
        Proj([exprs, _]) => x(exprs)?,
        Agg([exprs, group_keys, _]) => concat(x(exprs)?, x(group_keys)?),
        Empty(ids) => {
            let mut s = vec![];
            for id in ids.iter() {
                s.extend(x(id)?);
            }
            s
        }

        // prune node may changes the schema, but we don't know the exact result for now
        // so just return `None` to indicate "unknown"
        Prune(_) => return None,

        // not plan node
        _ => return None,
    })
}

/// Returns true if the schema of two nodes is equal.
fn schema_is_eq(v1: &str, v2: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let v1 = var(v1);
    let v2 = var(v2);
    move |egraph, _, subst| {
        let s1 = &egraph[subst[v1]].data.schema;
        let s2 = &egraph[subst[v2]].data.schema;
        s1.is_some() && s1 == s2
    }
}

#[cfg(test)]
mod tests {
    use super::ColumnIndexResolver;

    macro_rules! test_resolve_column_index {
        ($name:ident,rewrite: $input:expr,schema: $schema:expr,expect: $expected:expr,) => {
            #[test]
            fn $name() {
                let input = $input.parse().unwrap();
                let schema = $schema.parse().unwrap();
                let actual = ColumnIndexResolver::new(&schema).resolve(&input);
                assert_eq!(actual.to_string(), $expected);
            }
        };
    }

    test_resolve_column_index!(
        resolve_column_index1,
        rewrite: "(list (+ (+ $1.2 1) (sum $1.1)))",
        schema: "(list (+ $1.2 1) (sum $1.1) $1.2)",
        expect: "(list (+ #0 #1))",
    );
}
