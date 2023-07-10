// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use egg::Id;
use pretty_xmlish::helper::delegate_fmt;
use pretty_xmlish::Pretty;

use super::{Expr, RecExpr};
use crate::catalog::RootCatalog;

trait Insertable<'a> {
    fn with_cost(self, value: Option<f32>) -> Self;
}

impl<'a> Insertable<'a> for Vec<(&'a str, Pretty<'a>)> {
    fn with_cost(mut self, value: Option<f32>) -> Self {
        if let Some(value) = value {
            self.push(("cost", Pretty::display(&value)));
        }
        self
    }
}

/// A wrapper over [`RecExpr`] to explain it in [`Display`].
///
/// # Example
/// ```
/// use risinglight::planner::{Explain, RecExpr};
/// let expr: RecExpr = "(+ 1 2)".parse().unwrap();
/// println!("{}", Explain::of(&expr));
/// ```
pub struct Explain<'a> {
    expr: &'a RecExpr,
    costs: Option<&'a [f32]>,
    catalog: Option<&'a RootCatalog>,
    id: Id,
}

impl<'a> Explain<'a> {
    /// Create a [`Explain`] over [`RecExpr`].
    pub fn of(expr: &'a RecExpr) -> Self {
        Self {
            expr,
            costs: None,
            catalog: None,
            id: Id::from(expr.as_ref().len() - 1),
        }
    }

    /// Explain with costs.
    pub fn with_costs(mut self, costs: &'a [f32]) -> Self {
        self.costs = Some(costs);
        self
    }

    /// Explain column in name.
    pub fn with_catalog(mut self, catalog: &'a RootCatalog) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Returns a explain for the sub expression.
    #[inline]
    const fn expr(&self, id: &Id) -> Self {
        Explain {
            expr: self.expr,
            costs: self.costs,
            catalog: self.catalog,
            id: *id,
        }
    }

    /// Returns a explain for the child plan.
    #[inline]
    const fn child(&self, id: &Id) -> Self {
        Explain {
            expr: self.expr,
            costs: self.costs,
            catalog: self.catalog,
            id: *id,
        }
    }

    /// Returns a struct displaying the cost.
    #[inline]
    fn cost(&self) -> Option<f32> {
        self.costs.map(|cs| cs[usize::from(self.id)])
    }

    /// Returns whether the expression is `true`.
    #[inline]
    fn is_true(&self, id: &Id) -> bool {
        self.expr[*id] == Expr::true_()
    }
    /// Transforms the plan to `Pretty`, an intermediate representation for pretty printing. It will
    /// be printed to string later.
    pub fn pretty(&self) -> Pretty<'a> {
        use Expr::*;
        let enode = &self.expr[self.id];
        let cost = self.cost();
        match enode {
            Constant(v) => Pretty::display(v),
            Type(t) => Pretty::display(t),
            Table(i) => {
                if let Some(catalog) = self.catalog {
                    catalog.get_table(i).expect("no table").name().into()
                } else {
                    Pretty::display(i)
                }
            }
            Column(i) => {
                if let Some(catalog) = self.catalog {
                    let column_catalog = catalog.get_column(i).expect("no column");
                    column_catalog.into_name().into()
                } else {
                    Pretty::display(i)
                }
            }
            ColumnIndex(i) => Pretty::display(i),

            // TODO: use object
            ExtSource(src) => format!("path={:?}, format={}", src.path, src.format).into(),
            Symbol(s) => Pretty::display(s),
            Ref(e) => self.expr(e).pretty(),
            List(list) => Pretty::Array(list.iter().map(|e| self.expr(e).pretty()).collect()),

            // binary operations
            Add([a, b]) | Sub([a, b]) | Mul([a, b]) | Div([a, b]) | Mod([a, b])
            | StringConcat([a, b]) | Gt([a, b]) | Lt([a, b]) | GtEq([a, b]) | LtEq([a, b])
            | Eq([a, b]) | NotEq([a, b]) | And([a, b]) | Or([a, b]) | Xor([a, b])
            | Like([a, b]) => Pretty::childless_record(
                enode.to_string(),
                vec![
                    ("lhs", self.expr(a).pretty()),
                    ("rhs", self.expr(b).pretty()),
                ],
            ),

            // unary operations
            Neg(a) | Not(a) | IsNull(a) => {
                let name = enode.to_string();
                let v = vec![self.expr(a).pretty()];
                Pretty::fieldless_record(name, v)
            }

            If([cond, then, else_]) => Pretty::childless_record(
                "If",
                vec![
                    ("cond", self.expr(cond).pretty()),
                    ("then", self.expr(then).pretty()),
                    ("else", self.expr(else_).pretty()),
                ],
            ),

            // functions
            Extract([field, e]) => Pretty::childless_record(
                "Extract",
                vec![
                    ("from", self.expr(e).pretty()),
                    ("field", self.expr(field).pretty()),
                ],
            ),
            Field(field) => Pretty::display(field),
            Replace([a, b, c]) => Pretty::childless_record(
                "Replace",
                vec![
                    ("in", self.expr(a).pretty()),
                    ("from", self.expr(b).pretty()),
                    ("to", self.expr(c).pretty()),
                ],
            ),

            // aggregations
            RowCount | RowNumber => enode.to_string().into(),
            Max(a) | Min(a) | Sum(a) | Avg(a) | Count(a) | First(a) | Last(a) => {
                let name = enode.to_string();
                let v = vec![self.expr(a).pretty()];
                Pretty::fieldless_record(name, v)
            }
            Over([f, orderby, partitionby]) => Pretty::simple_record(
                "Over",
                vec![
                    ("order_by", self.expr(orderby).pretty()),
                    ("partition_by", self.expr(partitionby).pretty()),
                ],
                vec![self.expr(f).pretty()],
            ),

            Exists(a) => {
                let v = vec![self.expr(a).pretty()];
                Pretty::fieldless_record("Exists", v)
            }
            In([a, b]) => Pretty::simple_record(
                "In",
                vec![("in", self.expr(b).pretty())],
                vec![self.expr(a).pretty()],
            ),
            Cast([a, b]) => Pretty::simple_record(
                "Cast",
                vec![("type", self.expr(b).pretty())],
                vec![self.expr(a).pretty()],
            ),

            Scan([table, list]) | Internal([table, list]) => Pretty::childless_record(
                "Scan",
                vec![
                    ("table", self.expr(table).pretty()),
                    ("list", self.expr(list).pretty()),
                ]
                .with_cost(cost),
            ),
            Values(rows) => Pretty::simple_record(
                "Values",
                vec![("rows", Pretty::display(&rows.len()))].with_cost(cost),
                rows.iter().map(|id| self.expr(id).pretty()).collect(),
            ),
            Proj([exprs, child]) => Pretty::simple_record(
                "Projection",
                vec![("exprs", self.expr(exprs).pretty())].with_cost(cost),
                vec![self.child(child).pretty()],
            ),
            Filter([cond, child]) => Pretty::simple_record(
                "Filter",
                vec![("cond", self.expr(cond).pretty())].with_cost(cost),
                vec![self.child(child).pretty()],
            ),
            Order([orderby, child]) => Pretty::simple_record(
                "Order",
                vec![("by", self.expr(orderby).pretty())].with_cost(cost),
                vec![self.child(child).pretty()],
            ),
            Asc(a) | Desc(a) => {
                let name = enode.to_string();
                let v = vec![self.expr(a).pretty()];
                Pretty::fieldless_record(name, v)
            }
            Limit([limit, offset, child]) => Pretty::simple_record(
                "Limit",
                vec![
                    ("limit", self.expr(limit).pretty()),
                    ("offset", self.expr(offset).pretty()),
                ]
                .with_cost(cost),
                vec![self.child(child).pretty()],
            ),
            TopN([limit, offset, orderby, child]) => Pretty::simple_record(
                "TopN",
                vec![
                    ("limit", self.expr(limit).pretty()),
                    ("offset", self.expr(offset).pretty()),
                    ("order_by", self.expr(orderby).pretty()),
                ]
                .with_cost(cost),
                vec![self.child(child).pretty()],
            ),
            Join([ty, cond, left, right]) => {
                let mut fields = vec![("type", self.expr(ty).pretty())];

                if !self.is_true(cond) {
                    fields.push(("on", self.expr(cond).pretty()));
                }
                Pretty::simple_record(
                    "Join",
                    fields.with_cost(cost),
                    vec![self.child(left).pretty(), self.child(right).pretty()],
                )
            }
            HashJoin([ty, lkeys, rkeys, left, right]) => {
                let fields = vec![
                    ("lhs", self.expr(lkeys).pretty()),
                    ("rhs", self.expr(rkeys).pretty()),
                ];
                let eq = Pretty::childless_record("=", fields);
                let fields = vec![("type", self.expr(ty).pretty()), ("on", eq)].with_cost(cost);
                let children = vec![self.child(left).pretty(), self.child(right).pretty()];
                Pretty::simple_record("HashJoin", fields, children)
            }
            Inner | LeftOuter | RightOuter | FullOuter => Pretty::display(enode),
            Agg([aggs, group_keys, child]) => Pretty::simple_record(
                "Aggregate",
                vec![
                    ("aggs", self.expr(aggs).pretty()),
                    ("group_by", self.expr(group_keys).pretty()),
                ]
                .with_cost(cost),
                vec![self.child(child).pretty()],
            ),
            Window([windows, child]) => Pretty::simple_record(
                "Window",
                vec![("windows", self.expr(windows).pretty())].with_cost(cost),
                vec![self.child(child).pretty()],
            ),
            CreateTable(t) => {
                let fields = t.pretty_table().with_cost(cost);
                Pretty::childless_record("CreateTable", fields)
            }
            Drop(t) => {
                let fields = t.pretty_table().with_cost(cost);
                Pretty::childless_record("Drop", fields)
            }
            Insert([table, cols, child]) => Pretty::simple_record(
                "Insert",
                vec![
                    ("table", self.expr(table).pretty()),
                    ("cols", self.expr(cols).pretty()),
                ]
                .with_cost(cost),
                vec![self.child(child).pretty()],
            ),
            Delete([table, child]) => Pretty::simple_record(
                "Delete",
                vec![("table", self.expr(table).pretty())].with_cost(cost),
                vec![self.child(child).pretty()],
            ),
            CopyFrom([src, _]) => Pretty::childless_record(
                "CopyFrom",
                vec![("src", self.expr(src).pretty())].with_cost(cost),
            ),
            CopyTo([dst, child]) => Pretty::simple_record(
                "CopyTo",
                vec![("dst", self.expr(dst).pretty())].with_cost(cost),
                vec![self.child(child).pretty()],
            ),
            Explain(child) => Pretty::simple_record(
                "Explain",
                vec![].with_cost(cost),
                vec![self.child(child).pretty()],
            ),
            Empty(_) => Pretty::childless_record("Empty", vec![].with_cost(cost)),
        }
    }
}

impl<'a> fmt::Display for Explain<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        delegate_fmt(&self.pretty(), f, String::with_capacity(4096))
    }
}
