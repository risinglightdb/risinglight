use super::*;

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
        Proj([exprs, _]) | ProjAgg([exprs, _, _]) => x(*exprs)?,
        Agg([exprs, group_keys, _]) => concat(x(*exprs)?, x(*group_keys)?),

        // prune node may changes the schema, but we don't know the exact result for now
        // so just return `None` to indicate "unknown"
        Prune(_) => return None,

        // not plan node
        _ => return None,
    })
}
