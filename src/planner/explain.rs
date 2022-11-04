use std::fmt::{Display, Formatter, Result};

use egg::Id;

use super::{Expr, RecExpr};

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
    id: Id,
    depth: u8,
}

impl<'a> Explain<'a> {
    /// Create a [`Explain`] over [`RecExpr`].
    pub fn of(expr: &'a RecExpr) -> Self {
        Self {
            expr,
            costs: None,
            id: Id::from(expr.as_ref().len() - 1),
            depth: 0,
        }
    }

    /// Create a [`Explain`] with costs.
    pub fn with_costs(expr: &'a RecExpr, costs: &'a [f32]) -> Self {
        Self {
            expr,
            costs: Some(costs),
            id: Id::from(expr.as_ref().len() - 1),
            depth: 0,
        }
    }

    /// Returns a explain for the sub expression.
    #[inline]
    const fn expr(&self, id: &Id) -> Self {
        Explain {
            expr: self.expr,
            costs: self.costs,
            id: *id,
            depth: self.depth,
        }
    }

    /// Returns a explain for the child plan.
    #[inline]
    const fn child(&self, id: &Id) -> Self {
        Explain {
            expr: self.expr,
            costs: self.costs,
            id: *id,
            depth: self.depth + 1,
        }
    }

    /// Returns a struct displaying the tabs.
    #[inline]
    const fn tab(&self) -> impl Display {
        struct Tab(u8);
        impl Display for Tab {
            fn fmt(&self, f: &mut Formatter<'_>) -> Result {
                for _ in 0..self.0 {
                    write!(f, "  ")?;
                }
                Ok(())
            }
        }
        Tab(self.depth)
    }

    /// Returns a struct displaying the cost.
    #[inline]
    fn cost(&self) -> impl Display {
        struct Cost(Option<f32>);
        impl Display for Cost {
            fn fmt(&self, f: &mut Formatter<'_>) -> Result {
                match self.0 {
                    Some(c) => write!(f, " (cost={c})"),
                    None => Ok(()),
                }
            }
        }
        Cost(self.costs.map(|cs| cs[usize::from(self.id)]))
    }

    /// Returns whether the expression is `true`.
    #[inline]
    fn is_true(&self, id: &Id) -> bool {
        self.expr[*id] == Expr::true_()
    }
}

impl Display for Explain<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        use Expr::*;
        let enode = &self.expr[self.id];
        let tab = self.tab();
        let cost = self.cost();
        match enode {
            Constant(v) => write!(f, "{v}"),
            Type(t) => write!(f, "{t}"),
            Table(i) => write!(f, "{i}"),
            Column(i) => write!(f, "{i}"),
            ColumnIndex(i) => write!(f, "{i}"),
            ExtSource(src) => write!(f, "path={:?}, format={}", src.path, src.format),
            Symbol(s) => write!(f, "{s}"),

            List(list) => {
                write!(f, "[")?;
                for (i, v) in list.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", self.expr(v))?;
                }
                write!(f, "]")
            }

            // binary operations
            Add([a, b]) | Sub([a, b]) | Mul([a, b]) | Div([a, b]) | Mod([a, b])
            | StringConcat([a, b]) | Gt([a, b]) | Lt([a, b]) | GtEq([a, b]) | LtEq([a, b])
            | Eq([a, b]) | NotEq([a, b]) | And([a, b]) | Or([a, b]) | Xor([a, b])
            | Like([a, b]) => write!(f, "({} {} {})", self.expr(a), enode, self.expr(b)),

            // unary operations
            Neg(a) | Not(a) | IsNull(a) => write!(f, "({} {})", enode, self.expr(a)),

            If([cond, then, else_]) => write!(
                f,
                "(if {} {} {})",
                self.expr(cond),
                self.expr(then),
                self.expr(else_)
            ),

            RowCount => write!(f, "rowcount"),
            Max(a) | Min(a) | Sum(a) | Avg(a) | Count(a) | First(a) | Last(a) => {
                write!(f, "{}({})", enode, self.expr(a))
            }

            Exists(a) => write!(f, "exists({})", self.expr(a)),
            In([a, b]) => write!(f, "({} in {})", self.expr(a), self.expr(b)),
            Cast([a, b]) => write!(f, "({} :: {})", self.expr(a), self.expr(b)),

            Select([distinct, projection, from, where_, groupby, having]) => write!(
                f,
                "{tab}Select:{cost}\n  {tab}distinct={}\n  {tab}projection={}\n  {tab}where={}\n  {tab}groupby={}\n  {tab}having={}\n{}",
                self.expr(distinct),
                self.expr(projection),
                self.expr(where_),
                self.expr(groupby),
                self.expr(having),
                self.child(from),
            ),
            Distinct(_) => todo!(),

            Scan(list) => writeln!(f, "{tab}Scan: {}{cost}", self.expr(list)),
            Values(rows) => writeln!(f, "{tab}Values: {} rows{cost}", rows.len()),
            Proj([exprs, child]) => write!(
                f,
                "{tab}Projection: {}{cost}\n{}",
                self.expr(exprs),
                self.child(child)
            ),
            Filter([cond, child]) => {
                write!(f, "{tab}Filter: {}{cost}\n{}", self.expr(cond), self.child(child))
            }
            Order([orderby, child]) => {
                write!(
                    f,
                    "{tab}Order: {}{cost}\n{}",
                    self.expr(orderby),
                    self.child(child)
                )
            }
            Asc(a) | Desc(a) => write!(f, "{} {}", self.expr(a), enode),
            Limit([limit, offset, child]) => write!(
                f,
                "{tab}Limit: limit={}, offset={}{cost}\n{}",
                self.expr(limit),
                self.expr(offset),
                self.child(child)
            ),
            TopN([limit, offset, orderby, child]) => write!(
                f,
                "{tab}TopN: limit={}, offset={}, orderby={}{cost}\n{}",
                self.expr(limit),
                self.expr(offset),
                self.expr(orderby),
                self.child(child)
            ),
            Join([ty, cond, left, right]) => {
                write!(f, "{tab}Join: {}", self.expr(ty))?;
                if !self.is_true(cond) {
                    write!(f, ", on={}", self.expr(cond))?;
                }
                write!(f, "{cost}\n{}{}", self.child(left), self.child(right))
            }
            HashJoin([ty, lkeys, rkeys, left, right]) => write!(
                f,
                "{tab}HashJoin: {}, lkey={}, rkey={}{cost}\n{}{}",
                self.expr(ty),
                self.expr(lkeys),
                self.expr(rkeys),
                self.child(left),
                self.child(right)
            ),
            Inner | LeftOuter | RightOuter | FullOuter | Cross => write!(f, "{}", enode),
            Agg([aggs, group_keys, child]) => write!(
                f,
                "{tab}Aggregate: {}, groupby={}{cost}\n{}",
                self.expr(aggs),
                self.expr(group_keys),
                self.child(child)
            ),
            CreateTable(t) => writeln!(f, "{tab}CreateTable: name={:?}, ...{cost}", t.table_name),
            Drop(t) => writeln!(f, "{tab}Drop: {}, ...{cost}", t.object),
            Insert([cols, child]) => write!(f, "{tab}Insert: {}{cost}\n{}", self.expr(cols), self.child(child)),
            Delete([table, child]) => write!(f, "{tab}Delete: from={}{cost}\n{}", self.expr(table), self.child(child)),
            CopyFrom(src) => writeln!(f, "{tab}CopyFrom: {}{cost}", self.expr(src)),
            CopyTo([dst, child]) => write!(f, "{tab}CopyTo: {}{cost}\n{}", self.expr(dst), self.child(child)),
            Explain(child) => write!(f, "{tab}Explain:{cost}\n{}", self.child(child)),
            Prune(_) => panic!("cannot explain Prune"),
        }
    }
}
