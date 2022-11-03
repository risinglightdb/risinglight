// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! Apply expressions on data chunks.

use std::borrow::Borrow;
use std::fmt;

use egg::{Id, Language};

use crate::array::*;
use crate::parser::{BinaryOperator, UnaryOperator};
use crate::planner::{Expr, RecExpr};
use crate::types::{Blob, ConvertError, DataType, DataTypeKind, DataValue, Date, F64};

pub struct ExprRef<'a> {
    expr: &'a RecExpr,
    id: Id,
}

impl fmt::Display for ExprRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let recexpr = self.node().build_recexpr(|id| self.expr[id].clone());
        write!(f, "{recexpr}")
    }
}

impl<'a> ExprRef<'a> {
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
                array.try_cast(self.next(*ty).node().as_type())
            }
            IsNull(a) => {
                let array = self.next(*a).eval(chunk)?;
                Ok(ArrayImpl::new_bool(
                    (0..array.len())
                        .map(|i| array.get(i) == DataValue::Null)
                        .collect(),
                ))
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
}
