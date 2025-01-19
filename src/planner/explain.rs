// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use egg::Id;
use pretty_xmlish::helper::delegate_fmt;
use pretty_xmlish::Pretty;

use super::{Expr, RecExpr};
use crate::catalog::RootCatalog;

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
    id: Id,
    // additional context
    metadata: Option<&'a (dyn Fn(Id) -> Vec<(&'static str, String)> + Send + Sync)>,
    catalog: Option<&'a RootCatalog>,
}

impl<'a> Explain<'a> {
    /// Create a [`Explain`] over [`RecExpr`].
    pub fn of(expr: &'a RecExpr) -> Self {
        Self {
            expr,
            id: Id::from(expr.as_ref().len() - 1),
            metadata: None,
            catalog: None,
        }
    }

    /// Append metadata to each plan node.
    ///
    /// You should give a function that returns a map of metadata for the given node.
    pub fn with_metadata(
        mut self,
        f: &'a (dyn Fn(Id) -> Vec<(&'static str, String)> + Send + Sync),
    ) -> Self {
        self.metadata = Some(f);
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
            id: *id,
            metadata: self.metadata,
            catalog: self.catalog,
        }
    }

    /// Returns a explain for the child plan.
    #[inline]
    const fn child(&self, id: &Id) -> Self {
        Explain {
            expr: self.expr,
            id: *id,
            metadata: self.metadata,
            catalog: self.catalog,
        }
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

        // helper function to add metadata to the fields
        let with_meta = |mut fields: Vec<(&'a str, Pretty<'a>)>| {
            if let Some(f) = self.metadata {
                let meta = f(self.id);
                fields.extend(meta.into_iter().map(|(k, v)| (k, v.into())));
            }
            fields
        };

        match enode {
            Constant(v) => Pretty::display(v),
            Type(t) => Pretty::display(t),
            Table(i) => {
                if let Some(catalog) = self.catalog {
                    catalog
                        .get_table(i)
                        .expect("no table")
                        .name()
                        .to_string()
                        .into()
                } else {
                    Pretty::display(i)
                }
            }
            Column(i) => {
                if let Some(catalog) = self.catalog {
                    let column_catalog = catalog.get_column(i).expect("no column");
                    let mut name = column_catalog.into_name();
                    if i.table_occurrence != 0 {
                        name = format!("{}({})", name, i.table_occurrence);
                    }
                    name.into()
                } else {
                    Pretty::display(i)
                }
            }
            ColumnIndex(i) => Pretty::display(i),

            // TODO: use object
            ExtSource(src) => format!("path={:?}, format={}", src.path, src.format).into(),
            Symbol(s) => Pretty::display(s),
            Ref(e) => Pretty::fieldless_record("ref", vec![self.expr(e).pretty()]),
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
            Substring([str, start, len]) => Pretty::childless_record(
                "Substring",
                vec![
                    ("str", self.expr(str).pretty()),
                    ("start", self.expr(start).pretty()),
                    ("length", self.expr(len).pretty()),
                ],
            ),

            // vector functions
            VectorL2Distance([a, b]) => Pretty::childless_record(
                "VectorL2Distance",
                vec![
                    ("lhs", self.expr(a).pretty()),
                    ("rhs", self.expr(b).pretty()),
                ],
            ),
            VectorCosineDistance([a, b]) => Pretty::childless_record(
                "VectorCosineDistance",
                vec![
                    ("lhs", self.expr(a).pretty()),
                    ("rhs", self.expr(b).pretty()),
                ],
            ),
            VectorNegtiveInnerProduct([a, b]) => Pretty::childless_record(
                "VectorDotProduct",
                vec![
                    ("lhs", self.expr(a).pretty()),
                    ("rhs", self.expr(b).pretty()),
                ],
            ),

            // aggregations
            RowCount | RowNumber => enode.to_string().into(),
            Max(a) | Min(a) | Sum(a) | Avg(a) | Count(a) | First(a) | Last(a)
            | CountDistinct(a) => {
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

            Scan([table, list, filter]) => Pretty::childless_record(
                "Scan",
                with_meta(vec![
                    ("table", self.expr(table).pretty()),
                    ("list", self.expr(list).pretty()),
                    ("filter", self.expr(filter).pretty()),
                ]),
            ),
            IndexScan([table, columns, filter, op, key, vector]) => Pretty::childless_record(
                "IndexScan",
                with_meta(vec![
                    ("table", self.expr(table).pretty()),
                    ("columns", self.expr(columns).pretty()),
                    ("filter", self.expr(filter).pretty()),
                    ("op", self.expr(op).pretty()),
                    ("key", self.expr(key).pretty()),
                    ("vector", self.expr(vector).pretty()),
                ]),
            ),
            Values(values) => Pretty::simple_record(
                "Values",
                with_meta(vec![("rows", Pretty::display(&values.len()))]),
                values.iter().map(|id| self.expr(id).pretty()).collect(),
            ),
            Proj([exprs, child]) => Pretty::simple_record(
                "Projection",
                with_meta(vec![("exprs", self.expr(exprs).pretty())]),
                vec![self.child(child).pretty()],
            ),
            Filter([cond, child]) => Pretty::simple_record(
                "Filter",
                with_meta(vec![("cond", self.expr(cond).pretty())]),
                vec![self.child(child).pretty()],
            ),
            Order([orderby, child]) => Pretty::simple_record(
                "Order",
                with_meta(vec![("by", self.expr(orderby).pretty())]),
                vec![self.child(child).pretty()],
            ),
            Desc(a) => {
                let v = vec![self.expr(a).pretty()];
                Pretty::fieldless_record("desc", v)
            }
            Limit([limit, offset, child]) => Pretty::simple_record(
                "Limit",
                with_meta(vec![
                    ("limit", self.expr(limit).pretty()),
                    ("offset", self.expr(offset).pretty()),
                ]),
                vec![self.child(child).pretty()],
            ),
            TopN([limit, offset, orderby, child]) => Pretty::simple_record(
                "TopN",
                with_meta(vec![
                    ("limit", self.expr(limit).pretty()),
                    ("offset", self.expr(offset).pretty()),
                    ("order_by", self.expr(orderby).pretty()),
                ]),
                vec![self.child(child).pretty()],
            ),
            Join([ty, cond, left, right]) => {
                let mut fields = vec![("type", self.expr(ty).pretty())];

                if !self.is_true(cond) {
                    fields.push(("on", self.expr(cond).pretty()));
                }
                Pretty::simple_record(
                    "Join",
                    with_meta(fields),
                    vec![self.child(left).pretty(), self.child(right).pretty()],
                )
            }
            HashJoin([ty, cond, lkeys, rkeys, left, right])
            | MergeJoin([ty, cond, lkeys, rkeys, left, right]) => {
                let name = match enode {
                    HashJoin(_) => "HashJoin",
                    MergeJoin(_) => "MergeJoin",
                    _ => unreachable!(),
                };
                let fields = with_meta(vec![
                    ("type", self.expr(ty).pretty()),
                    ("cond", self.expr(cond).pretty()),
                    ("lkey", self.expr(lkeys).pretty()),
                    ("rkey", self.expr(rkeys).pretty()),
                ]);
                let children = vec![self.child(left).pretty(), self.child(right).pretty()];
                Pretty::simple_record(name, fields, children)
            }
            Apply([ty, left, right]) => Pretty::simple_record(
                "Apply",
                with_meta(vec![("type", self.expr(ty).pretty())]),
                vec![self.child(left).pretty(), self.child(right).pretty()],
            ),
            Inner | LeftOuter | RightOuter | FullOuter | Semi | Anti => Pretty::display(enode),
            Agg([aggs, child]) => Pretty::simple_record(
                "Agg",
                with_meta(vec![("aggs", self.expr(aggs).pretty())]),
                vec![self.child(child).pretty()],
            ),
            HashAgg([keys, aggs, child]) | SortAgg([keys, aggs, child]) => Pretty::simple_record(
                match enode {
                    HashAgg(_) => "HashAgg",
                    SortAgg(_) => "SortAgg",
                    _ => unreachable!(),
                },
                with_meta(vec![
                    ("keys", self.expr(keys).pretty()),
                    ("aggs", self.expr(aggs).pretty()),
                ]),
                vec![self.child(child).pretty()],
            ),
            Window([windows, child]) => Pretty::simple_record(
                "Window",
                with_meta(vec![("windows", self.expr(windows).pretty())]),
                vec![self.child(child).pretty()],
            ),
            CreateTable(t) => {
                let fields = with_meta(t.pretty_table());
                Pretty::childless_record("CreateTable", fields)
            }
            CreateIndex(i) => {
                let fields = with_meta(i.pretty_index());
                Pretty::childless_record("CreateIndex", fields)
            }
            CreateView([table, query]) => Pretty::simple_record(
                "CreateView",
                with_meta(vec![("table", self.expr(table).pretty())]),
                vec![self.expr(query).pretty()],
            ),
            CreateFunction(f) => {
                let v = f.pretty_function();
                Pretty::childless_record("CreateFunction", v)
            }
            Drop(tables) => {
                let fields = with_meta(vec![("objects", self.expr(tables).pretty())]);
                Pretty::childless_record("Drop", fields)
            }
            Insert([table, cols, child]) => Pretty::simple_record(
                "Insert",
                with_meta(vec![
                    ("table", self.expr(table).pretty()),
                    ("cols", self.expr(cols).pretty()),
                ]),
                vec![self.child(child).pretty()],
            ),
            Delete([table, child]) => Pretty::simple_record(
                "Delete",
                with_meta(vec![("table", self.expr(table).pretty())]),
                vec![self.child(child).pretty()],
            ),
            CopyFrom([src, _]) => Pretty::childless_record(
                "CopyFrom",
                with_meta(vec![("src", self.expr(src).pretty())]),
            ),
            CopyTo([dst, child]) => Pretty::simple_record(
                "CopyTo",
                with_meta(vec![("dst", self.expr(dst).pretty())]),
                vec![self.child(child).pretty()],
            ),
            Explain(child) => Pretty::simple_record(
                "Explain",
                with_meta(vec![]),
                vec![self.child(child).pretty()],
            ),
            Analyze(child) => Pretty::simple_record(
                "Analyze",
                with_meta(vec![]),
                vec![self.child(child).pretty()],
            ),
            Pragma([name, value]) => Pretty::childless_record(
                "Pragma",
                with_meta(vec![
                    ("name", self.expr(name).pretty()),
                    ("value", self.expr(value).pretty()),
                ]),
            ),
            Set([name, value]) => Pretty::childless_record(
                "Set",
                with_meta(vec![
                    ("name", self.expr(name).pretty()),
                    ("value", self.expr(value).pretty()),
                ]),
            ),
            Empty(_) => Pretty::childless_record("Empty", with_meta(vec![])),
            Max1Row(child) => Pretty::fieldless_record("Max1Row", vec![self.expr(child).pretty()]),
        }
    }
}

impl fmt::Display for Explain<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        delegate_fmt(&self.pretty(), f, String::with_capacity(4096))
    }
}
