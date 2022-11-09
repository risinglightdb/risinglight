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
//! | [`type_`]  |                       | data type                     | [`Type`]       |
//! | [`rows`]   |                       | estimated rows                | [`Rows`]       |
//!
//! It would be best if you have a background in program analysis.
//! Here is a recommended course: <https://pascal-group.bitbucket.io/teaching.html>.
//!
//! [`ConstValue`]: expr::ConstValue
//! [`ColumnSet`]: plan::ColumnSet
//! [`AggSet`]: agg::AggSet
//! [`Schema`]: schema::Schema
//! [`Type`]: type_::Type
//! [`Rows`]: rows::Rows

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::LazyLock;

use egg::{rewrite as rw, *};

use super::{EGraph, Expr, Pattern, RecExpr, Rewrite};
use crate::catalog::RootCatalogRef;
use crate::types::F32;

mod agg;
mod expr;
mod plan;
mod rows;
mod schema;
mod type_;

pub use self::schema::ColumnIndexResolver;
pub use self::type_::TypeError;

/// Stage1 rules in the optimizer.
pub static STAGE1_RULES: LazyLock<Vec<Rewrite>> = LazyLock::new(|| {
    let mut rules = vec![];
    rules.append(&mut plan::column_prune_rules());
    rules.append(&mut schema::rules());
    rules
});

/// Stage2 rules in the optimizer.
pub static STAGE2_RULES: LazyLock<Vec<Rewrite>> = LazyLock::new(|| {
    let mut rules = vec![];
    rules.append(&mut expr::rules());
    rules.append(&mut plan::always_better_rules());
    rules
});

/// Stage3 rules in the optimizer.
pub static STAGE3_RULES: LazyLock<Vec<Rewrite>> = LazyLock::new(|| {
    let mut rules = vec![];
    rules.append(&mut expr::rules());
    rules.append(&mut plan::join_rules());
    rules
});

/// The unified analysis for all rules.
#[derive(Default)]
pub struct ExprAnalysis;

/// The analysis data associated with each eclass.
///
/// See [`egg::Analysis`] for how data is being processed.
#[derive(Debug)]
pub struct Data {
    /// Some if the expression is a constant.
    pub constant: expr::ConstValue,

    /// All columns involved in the node.
    pub columns: plan::ColumnSet,

    /// The schema for plan node: a list of expressions.
    ///
    /// For non-plan node, it always be None.
    /// For plan node, it may be None if the schema is unknown due to unresolved `prune`.
    pub schema: schema::Schema,
    /// Estimate rows.
    pub rows: rows::Rows,
}

impl Analysis<Expr> for ExprAnalysis {
    type Data = Data;

    /// Analyze a node and give the result.
    fn make(egraph: &EGraph, enode: &Expr) -> Self::Data {
        Data {
            constant: expr::eval_constant(egraph, enode),
            columns: plan::analyze_columns(egraph, enode),
            schema: schema::analyze_schema(enode, |i| egraph[*i].data.schema.clone()),
            rows: rows::analyze_rows(egraph, enode),
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
        let merge_schema = egg::merge_max(&mut to.schema, from.schema);
        let merge_rows = egg::merge_min(
            unsafe { std::mem::transmute(&mut to.rows) },
            F32::from(from.rows),
        );
        merge_const | merge_columns | merge_schema | merge_rows
    }

    /// Modify the graph after analyzing a node.
    fn modify(egraph: &mut EGraph, id: Id) {
        expr::union_constant(egraph, id);
    }
}

/// Analysis used in binding and building executor.
#[derive(Default)]
pub struct TypeSchemaAnalysis {
    pub catalog: RootCatalogRef,
}

#[derive(Debug)]
pub struct TypeSchema {
    /// Data type of the expression.
    pub type_: type_::Type,

    /// The schema for plan node: a list of expressions.
    ///
    /// For non-plan node, it always be None.
    /// For plan node, it may be None if the schema is unknown due to unresolved `prune`.
    pub schema: schema::Schema,

    /// All aggragations in the tree.
    pub aggs: agg::AggSet,
}

impl Analysis<Expr> for TypeSchemaAnalysis {
    type Data = TypeSchema;

    fn make(egraph: &egg::EGraph<Expr, Self>, enode: &Expr) -> Self::Data {
        TypeSchema {
            type_: type_::analyze_type(
                enode,
                |i| egraph[*i].data.type_.clone(),
                &egraph.analysis.catalog,
            ),
            schema: schema::analyze_schema(enode, |i| egraph[*i].data.schema.clone()),
            aggs: agg::analyze_aggs(enode, |i| egraph[*i].data.aggs.clone()),
        }
    }

    fn merge(&mut self, to: &mut Self::Data, from: Self::Data) -> DidMerge {
        let merge_type = egg::merge_max(&mut to.type_, from.type_);
        let merge_schema = egg::merge_max(&mut to.schema, from.schema);
        let merge_aggs = egg::merge_max(&mut to.aggs, from.aggs);
        merge_type | merge_schema | merge_aggs
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
fn pattern(s: &str) -> Pattern {
    s.parse().expect("invalid pattern")
}
