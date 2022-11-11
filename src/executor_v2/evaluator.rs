// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! Apply expressions on data chunks.

use std::fmt;

use egg::{Id, Language};
use itertools::Itertools;

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
                // TODO: optimize this
                for _ in 0..chunk.cardinality() {
                    builder.push(v);
                }
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
            Asc(a) | Desc(a) | Nested(a) => self.next(*a).eval(chunk),
            // for aggs, evaluate its children
            RowCount => Ok(ArrayImpl::new_null(
                (0..chunk.cardinality()).map(|_| ()).collect(),
            )),
            Count(a) | Sum(a) | Min(a) | Max(a) | First(a) | Last(a) => self.next(*a).eval(chunk),
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
            RowCount | Count(_) => DataValue::Int32(0),
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
        for (state, id) in states.iter_mut().zip_eq(list) {
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
        for ((state, id), value) in states.iter_mut().zip_eq(list).zip_eq(values) {
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
            Count(a) => Ok(state.add(DataValue::Int32(
                self.next(*a).eval(chunk)?.get_valid_bitmap().count_ones() as _,
            ))),
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
            RowCount => state.add(DataValue::Int32(1)),
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
            .map(|id| match self.next(*id).node() {
                Expr::Asc(_) => false,
                Expr::Desc(_) => true,
                _ => panic!("not order"),
            })
            .collect()
    }
}
