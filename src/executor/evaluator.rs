// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! Apply expressions on data chunks.

use std::fmt;

use egg::{Id, Language};

use crate::array::*;
use crate::planner::{Expr, RecExpr};
use crate::types::{ConvertError, DataValue};

/// A wrapper over [`RecExpr`] to evaluate it on [`DataChunk`]s.
pub struct Evaluator<'a> {
    expr: &'a RecExpr,
    id: Id,
}

impl fmt::Display for Evaluator<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let recexpr = self.node().build_recexpr(|id| self.expr[id].clone());
        write!(f, "{recexpr}")
    }
}

impl<'a> Evaluator<'a> {
    /// Create a [`Evaluator`] over [`RecExpr`].
    pub fn new(expr: &'a RecExpr) -> Self {
        Self {
            expr,
            id: Id::from(expr.as_ref().len() - 1),
        }
    }

    fn node(&self) -> &Expr {
        &self.expr[self.id]
    }

    fn next(&self, id: Id) -> Self {
        Self {
            expr: self.expr,
            id,
        }
    }

    /// Evaluate a list of expressions.
    pub fn eval_list(&self, chunk: &DataChunk) -> Result<DataChunk, ConvertError> {
        let list = self.node().as_list();
        if list.is_empty() {
            return Ok(DataChunk::no_column(chunk.cardinality()));
        }
        list.iter().map(|id| self.next(*id).eval(chunk)).collect()
    }

    /// Evaluate the given expression as an array.
    pub fn eval(&self, chunk: &DataChunk) -> Result<ArrayImpl, ConvertError> {
        use Expr::*;
        match self.node() {
            ColumnIndex(idx) => Ok(chunk.array_at(idx.0 as _).clone()),
            Constant(v) => {
                let mut builder =
                    ArrayBuilderImpl::with_capacity(chunk.cardinality(), &v.data_type());
                builder.push_n(chunk.cardinality(), v);
                Ok(builder.finish())
            }
            Cast([ty, a]) => {
                let array = self.next(*a).eval(chunk)?;
                array.cast(self.next(*ty).node().as_type())
            }
            IsNull(a) => {
                let array = self.next(*a).eval(chunk)?;
                Ok(ArrayImpl::new_bool(
                    array.get_valid_bitmap().iter().map(|v| !v).collect(),
                ))
            }
            Like([a, b]) => match self.next(*b).node() {
                Expr::Constant(DataValue::String(pattern)) => {
                    let a = self.next(*a).eval(chunk)?;
                    a.like(pattern)
                }
                _ => panic!("like pattern must be a string constant"),
            },
            Extract([field, a]) => {
                let a = self.next(*a).eval(chunk)?;
                let Expr::Field(field) = self.expr[*field] else {
                    panic!("not a field")
                };
                a.extract(field)
            }
            Substring([str, start, length]) => {
                let str = self.next(*str).eval(chunk)?;
                let start = self.next(*start).eval(chunk)?;
                let length = self.next(*length).eval(chunk)?;
                str.substring(&start, &length)
            }
            If([cond, then, else_]) => {
                let cond = self.next(*cond).eval(chunk)?;
                let then = self.next(*then).eval(chunk)?;
                let else_ = self.next(*else_).eval(chunk)?;
                cond.select(&then, &else_)
            }
            In([expr, list]) => {
                let expr = self.next(*expr).eval(chunk)?;
                let values = self.next(*list).eval_list(chunk)?;
                let mut in_ = expr.eq(values.array_at(0))?;
                for value in &values.arrays()[1..] {
                    let eq = expr.eq(value)?;
                    in_ = in_.or(&eq).unwrap();
                }
                Ok(in_)
            }
            Desc(a) | Ref(a) => self.next(*a).eval(chunk),
            // for aggs, evaluate its children
            RowCount => Ok(ArrayImpl::new_null(
                (0..chunk.cardinality()).map(|_| ()).collect(),
            )),
            Count(a) | Sum(a) | Min(a) | Max(a) | First(a) | Last(a) => self.next(*a).eval(chunk),
            Replace([a, from, to]) => {
                let a = self.next(*a).eval(chunk)?;
                let from = self.next(*from);
                let from = match from.node() {
                    Expr::Constant(DataValue::String(s)) => s,
                    _ => panic!("replace from must be a string constant"),
                };
                let to = self.next(*to);
                let to = match to.node() {
                    Expr::Constant(DataValue::String(s)) => s,
                    _ => panic!("replace to must be a string constant"),
                };
                a.replace(from, to)
            }
            e => {
                if let Some((op, a, b)) = e.binary_op() {
                    let left = self.next(a).eval(chunk)?;
                    let right = self.next(b).eval(chunk)?;
                    left.binary_op(&op, &right)
                } else if let Some((op, a)) = e.unary_op() {
                    let array = self.next(a).eval(chunk)?;
                    array.unary_op(&op)
                } else {
                    panic!("can not evaluate expression: {self}");
                }
            }
        }
    }

    /// Returns the initial aggregation states.
    pub fn init_agg_states<B: FromIterator<DataValue>>(&self) -> B {
        (self.node().as_list().iter())
            .map(|id| self.next(*id).init_agg_state())
            .collect()
    }

    /// Returns the initial aggregation state.
    fn init_agg_state(&self) -> DataValue {
        use Expr::*;
        match self.node() {
            Over([window, _, _]) => self.next(*window).init_agg_state(),
            RowCount | RowNumber | Count(_) => DataValue::Int32(0),
            Sum(_) | Min(_) | Max(_) | First(_) | Last(_) => DataValue::Null,
            t => panic!("not aggregation: {t}"),
        }
    }

    /// Evaluate a list of aggregations.
    pub fn eval_agg_list(
        &self,
        states: &mut [DataValue],
        chunk: &DataChunk,
    ) -> Result<(), ConvertError> {
        let list = self.node().as_list();
        for (state, id) in states.iter_mut().zip(list) {
            *state = self.next(*id).eval_agg(state.clone(), chunk)?;
        }
        Ok(())
    }

    /// Append a list of values to a list of agg states.
    pub fn agg_list_append(
        &self,
        states: &mut [DataValue],
        values: impl Iterator<Item = DataValue>,
    ) {
        let list = self.node().as_list();
        for ((state, id), value) in states.iter_mut().zip(list).zip(values) {
            *state = self.next(*id).agg_append(state.clone(), value);
        }
    }

    /// Evaluate the aggregation.
    fn eval_agg(&self, state: DataValue, chunk: &DataChunk) -> Result<DataValue, ConvertError> {
        impl DataValue {
            fn add(self, other: Self) -> Self {
                if self.is_null() {
                    other
                } else {
                    self + other
                }
            }
            fn or(self, other: Self) -> Self {
                if self.is_null() {
                    other
                } else {
                    self
                }
            }
        }
        use Expr::*;
        match self.node() {
            RowCount => Ok(state.add(DataValue::Int32(chunk.cardinality() as _))),
            Count(a) => Ok(state.add(DataValue::Int32(self.next(*a).eval(chunk)?.count() as _))),
            Sum(a) => Ok(state.add(self.next(*a).eval(chunk)?.sum())),
            Min(a) => Ok(state.min(self.next(*a).eval(chunk)?.min_())),
            Max(a) => Ok(state.max(self.next(*a).eval(chunk)?.max_())),
            First(a) => Ok(state.or(self.next(*a).eval(chunk)?.first())),
            Last(a) => Ok(self.next(*a).eval(chunk)?.last().or(state)),
            t => panic!("not aggregation: {t}"),
        }
    }

    /// Append a value to agg state.
    fn agg_append(&self, state: DataValue, value: DataValue) -> DataValue {
        use Expr::*;
        match self.node() {
            Over([window, _, _]) => self.next(*window).agg_append(state, value),
            RowCount | RowNumber => state.add(DataValue::Int32(1)),
            Count(_) => state.add(DataValue::Int32(!value.is_null() as _)),
            Sum(_) => state.add(value),
            Min(_) => state.min(value),
            Max(_) => state.max(value),
            First(_) => state.or(value),
            Last(_) => value,
            t => panic!("not aggregation: {t}"),
        }
    }

    /// Returns a list of bools for order keys.
    ///
    /// The bool is false if the order is ascending, true if the order is descending.
    pub fn orders(&self) -> Vec<bool> {
        (self.node().as_list().iter())
            .map(|id| matches!(self.next(*id).node(), Expr::Desc(_)))
            .collect()
    }
}
