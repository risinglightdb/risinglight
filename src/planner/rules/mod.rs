// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! Optimization rules and related program analyses.
//!
//! Currently we have 6 kinds of analyses.
//! Each of them is defined in a sub-module:
//!
//! |   module   |         rules         |            analysis           | analysis data      |
//! |------------|-----------------------|-------------------------------|--------------------|
//! | [`expr`]   | expr simplification   | constant value                | [`ConstValue`]     |
//! | [`range`]  | filter scan rule      | range condition               | [`RangeCondition`] |
//! | [`plan`]   | plan optimization     | use and defination of columns | [`ColumnSet`]      |
//! | [`agg`]    | agg extraction        | aggregations in an expr       | [`AggSet`]         |
//! | [`schema`] | column id to index    | output schema of a plan       | [`Schema`]         |
//! | [`type_`]  |                       | data type                     | [`Type`]           |
//! | [`rows`]   |                       | estimated rows                | [`Rows`]           |
//! | [`order`]  | merge join            | ordered keys                  | [`OrderKey`]   |
//!
//! It would be best if you have a background in program analysis.
//! Here is a recommended course: <https://pascal-group.bitbucket.io/teaching.html>.
//!
//! [`ConstValue`]: expr::ConstValue
//! [`RangeCondition`]: range::RangeCondition
//! [`ColumnSet`]: plan::ColumnSet
//! [`AggSet`]: agg::AggSet
//! [`Schema`]: schema::Schema
//! [`Type`]: type_::Type
//! [`Rows`]: rows::Rows
//! [`OrderKey`]: order::OrderKey

use std::collections::HashSet;
use std::hash::Hash;

use egg::{rewrite as rw, *};

use super::{Config, EGraph, Expr, ExprExt, Pattern, Rewrite};
use crate::catalog::RootCatalogRef;
use crate::types::F32;

pub mod agg;
pub mod expr;
pub mod order;
pub mod plan;
pub mod range;
pub mod rows;
pub mod schema;
pub mod type_;

pub use rows::Statistics;

pub use self::type_::TypeError;

/// The unified analysis for all rules.
#[derive(Default, Clone)]
pub struct ExprAnalysis {
    pub catalog: RootCatalogRef,
    pub config: Config,
    pub stat: Statistics,
}

/// The analysis data associated with each eclass.
///
/// See [`egg::Analysis`] for how data is being processed.
#[derive(Debug)]
pub struct Data {
    /// Some if the expression is a constant.
    pub constant: expr::ConstValue,

    /// Some if the expression is a range condition.
    pub range: range::RangeCondition,

    /// For expression node, it is the set of columns used in the expression.
    /// For plan node, it is the set of columns produced by the plan.
    pub columns: plan::ColumnSet,

    /// A list of expressions produced by plan node.
    pub schema: schema::Schema,

    /// Estimate rows.
    pub rows: rows::Rows,

    /// Order key for plan node.
    pub orderby: order::OrderKey,
}

impl Analysis<Expr> for ExprAnalysis {
    type Data = Data;

    /// Analyze a node and give the result.
    fn make(egraph: &EGraph, enode: &Expr) -> Self::Data {
        Data {
            constant: expr::eval_constant(egraph, enode),
            range: range::analyze_range(egraph, enode),
            columns: plan::analyze_columns(egraph, enode),
            schema: schema::analyze_schema(
                enode,
                |id| egraph[*id].data.schema.clone(),
                |id| egraph[*id].nodes[0].clone(),
            ),
            rows: rows::analyze_rows(egraph, enode),
            orderby: order::analyze_order(egraph, enode),
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
        // if both are Some, choose arbitrary one. not sure whether it is safe.
        let merge_range =
            egg::merge_option(&mut to.range, from.range, |_, _| DidMerge(false, true));
        let merge_columns = merge_small_set(&mut to.columns, from.columns);
        let merge_schema = egg::merge_max(&mut to.schema, from.schema);
        let merge_rows = egg::merge_min(
            unsafe { std::mem::transmute::<&mut f32, &mut F32>(&mut to.rows) },
            F32::from(from.rows),
        );
        let merge_order = egg::merge_max(&mut to.orderby, from.orderby);
        merge_const | merge_range | merge_columns | merge_schema | merge_rows | merge_order
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

#[derive(Debug, Clone)]
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

    /// All over nodes in the tree.
    pub overs: agg::OverSet,
}

impl Analysis<Expr> for TypeSchemaAnalysis {
    type Data = TypeSchema;

    fn make(egraph: &egg::EGraph<Expr, Self>, enode: &Expr) -> Self::Data {
        TypeSchema {
            type_: type_::analyze_type(
                enode,
                |i| egraph[*i].data.type_.clone(),
                |id| egraph[*id].nodes[0].clone(),
                &egraph.analysis.catalog,
            ),
            schema: schema::analyze_schema(
                enode,
                |i| egraph[*i].data.schema.clone(),
                |id| egraph[*id].nodes[0].clone(),
            ),
            aggs: agg::analyze_aggs(enode, |i| egraph[*i].data.aggs.clone()),
            overs: agg::analyze_overs(enode, |i| egraph[*i].data.overs.clone()),
        }
    }

    fn merge(&mut self, to: &mut Self::Data, from: Self::Data) -> DidMerge {
        let merge_type = egg::merge_max(&mut to.type_, from.type_);
        let merge_schema = egg::merge_max(&mut to.schema, from.schema);
        let merge_aggs = egg::merge_max(&mut to.aggs, from.aggs);
        let merge_overs = egg::merge_max(&mut to.overs, from.overs);
        merge_type | merge_schema | merge_aggs | merge_overs
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
