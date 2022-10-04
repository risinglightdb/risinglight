use std::collections::HashSet;
use std::hash::Hash;

use egg::{rewrite as rw, *};

use super::Expr;

mod agg;
mod expr;
mod plan;
mod schema;

type EGraph = egg::EGraph<Expr, ExprAnalysis>;
type Rewrite = egg::Rewrite<Expr, ExprAnalysis>;
type RecExpr = egg::RecExpr<Expr>;

pub fn all_rules() -> Vec<Rewrite> {
    let mut rules = vec![];
    rules.extend(expr::rules());
    rules.extend(plan::rules());
    rules.extend(agg::rules());
    rules
}

/// Create a `Var` from string.
///
/// This is a helper function for submodules.
fn var(s: &str) -> Var {
    s.parse().expect("invalid variable")
}

/// Create a `Pattern` from string.
///
/// This is a helper function for submodules.
fn pattern(s: &str) -> Pattern<Expr> {
    s.parse().expect("invalid pattern")
}

#[derive(Default)]
pub struct ExprAnalysis;

#[derive(Debug)]
pub struct Data {
    /// Some if the expression is a constant.
    val: expr::ConstValue,
    /// All columns involved in the node.
    columns: plan::ColumnSet,
    /// All aggragations in the tree.
    aggs: agg::AggSet,
    /// The schema for plan node: a list of expressions.
    ///
    /// For non-plan node, it always be None.
    /// For plan node, it may be None if the schema is unknown due to unresolved `prune`.
    schema: schema::Schema,
}

impl Analysis<Expr> for ExprAnalysis {
    type Data = Data;

    fn merge(&mut self, to: &mut Self::Data, from: Self::Data) -> DidMerge {
        let merge_val = egg::merge_max(&mut to.val, from.val);
        let merge_col = merge_small_set(&mut to.columns, from.columns);
        let merge_agg = merge_small_set(&mut to.aggs, from.aggs);
        let merge_schema = egg::merge_max(&mut to.schema, from.schema);
        merge_val | merge_col | merge_agg | merge_schema
    }

    fn make(egraph: &EGraph, enode: &Expr) -> Self::Data {
        Data {
            val: expr::eval_constant(egraph, enode),
            columns: plan::analyze_columns(egraph, enode),
            aggs: agg::analyze_aggs(egraph, enode),
            schema: schema::analyze_schema(egraph, enode),
        }
    }

    fn modify(egraph: &mut EGraph, id: Id) {
        expr::modify(egraph, id);
    }
}

/// Merge 2 set.
fn merge_small_set<T: Eq + Hash>(to: &mut HashSet<T>, from: HashSet<T>) -> DidMerge {
    if from.len() < to.len() {
        *to = from;
        DidMerge(true, false)
    } else {
        DidMerge(false, true)
    }
}
