//! Optimization rules and related program analyses.
//!
//! Currently we have 4 kinds of rules.
//! Each of them is defined in a sub-module and has its own analysis:
//!
//! |   module   |         rules         |            analysis           | analysis data  |
//! |------------|-----------------------|-------------------------------|----------------|
//! | [`expr`]   | expr simplification   | constant value                | [`ConstValue`] |
//! | [`plan`]   | plan optimization     | use and defination of columns | [`ColumnSet`]  |
//! | [`agg`]    | agg extraction        | aggregations in an expr       | [`AggSet`]     |
//! | [`schema`] | column id to index    | output schema of a plan       | [`Schema`]     |
//!
//! It would be best if you have a background in program analysis.
//! Here is a recommended course: <https://pascal-group.bitbucket.io/teaching.html>.
//!
//! [`ConstValue`]: expr::ConstValue
//! [`ColumnSet`]: plan::ColumnSet
//! [`AggSet`]: agg::AggSet
//! [`Schema`]: schema::Schema

use std::collections::HashSet;
use std::hash::Hash;

use egg::{rewrite as rw, *};

use super::Expr;

mod agg;
mod expr;
mod plan;
mod schema;

// Alias types for our language.
type EGraph = egg::EGraph<Expr, ExprAnalysis>;
type Rewrite = egg::Rewrite<Expr, ExprAnalysis>;
type RecExpr = egg::RecExpr<Expr>;

/// Returns all rules in the optimizer.
pub fn all_rules() -> Vec<Rewrite> {
    let mut rules = vec![];
    rules.append(&mut expr::rules());
    rules.append(&mut plan::rules());
    rules.append(&mut agg::rules());
    rules
}

/// The unified analysis for all rules.
#[derive(Default)]
pub struct ExprAnalysis;

/// The analysis data associated with each eclass.
///
/// See [`egg::Analysis`] for how data is being processed.
#[derive(Debug)]
pub struct Data {
    /// Some if the expression is a constant.
    constant: expr::ConstValue,

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

    /// Analyze a node and give the result.
    fn make(egraph: &EGraph, enode: &Expr) -> Self::Data {
        Data {
            constant: expr::eval_constant(egraph, enode),
            columns: plan::analyze_columns(egraph, enode),
            aggs: agg::analyze_aggs(egraph, enode),
            schema: schema::analyze_schema(egraph, enode),
        }
    }

    /// Merge the analysis data with previous one.
    ///
    /// This process makes the analysis data more accurate.
    ///
    /// For example, if we have an expr `a + 1 - a`, the constant analysis will give a result `None`
    /// since we are not sure if it is a constant or not. But after we applied a rule and turned
    /// it to `a - a + 1` -> `0 + 1`, we know it is a constant. Then in this function, we merge the
    /// new result `Some(1)` with the previous `None` and keep `Some(1)` as the final result.
    fn merge(&mut self, to: &mut Self::Data, from: Self::Data) -> DidMerge {
        let merge_const = egg::merge_max(&mut to.constant, from.constant);
        let merge_columns = merge_small_set(&mut to.columns, from.columns);
        let merge_aggs = merge_small_set(&mut to.aggs, from.aggs);
        let merge_schema = egg::merge_max(&mut to.schema, from.schema);
        merge_const | merge_columns | merge_aggs | merge_schema
    }

    /// Modify the graph after analyzing a node.
    fn modify(egraph: &mut EGraph, id: Id) {
        expr::union_constant(egraph, id);
    }
}

/// Merge two result set and keep the smaller one.
fn merge_small_set<T: Eq + Hash>(to: &mut HashSet<T>, from: HashSet<T>) -> DidMerge {
    if from.len() < to.len() {
        *to = from;
        DidMerge(true, false)
    } else {
        DidMerge(false, true)
    }
}

/// Create a [`Var`] from string.
///
/// This is a helper function for submodules.
fn var(s: &str) -> Var {
    s.parse().expect("invalid variable")
}

/// Create a [`Pattern`] from string.
///
/// This is a helper function for submodules.
fn pattern(s: &str) -> Pattern<Expr> {
    s.parse().expect("invalid pattern")
}
