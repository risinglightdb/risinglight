// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! Apply expressions on data chunks.

use crate::array::*;
use crate::storage::PackedVec;
use crate::types::{ConvertError, DataValue};
use crate::v1::binder::BoundExpr;

impl BoundExpr {
    /// Evaluate the given expression as an array.
    pub fn eval(&self, chunk: &DataChunk) -> Result<ArrayImpl, ConvertError> {
        match &self {
            BoundExpr::InputRef(input_ref) => Ok(chunk.array_at(input_ref.index).clone()),
            BoundExpr::BinaryOp(binary_op) => {
                let left = binary_op.left_expr.eval(chunk)?;
                let right = binary_op.right_expr.eval(chunk)?;
                left.binary_op(&binary_op.op, &right)
            }
            BoundExpr::UnaryOp(op) => {
                let array = op.expr.eval(chunk)?;
                array.unary_op(&op.op)
            }
            BoundExpr::Constant(v) => {
                let mut builder =
                    ArrayBuilderImpl::with_capacity(chunk.cardinality(), &self.return_type());
                // TODO: optimize this
                for _ in 0..chunk.cardinality() {
                    builder.push(v);
                }
                Ok(builder.finish())
            }
            BoundExpr::TypeCast(cast) => {
                let array = cast.expr.eval(chunk)?;
                if self.return_type() == cast.expr.return_type() {
                    return Ok(array);
                }
                array.cast(&cast.ty)
            }
            BoundExpr::IsNull(expr) => {
                let array = expr.expr.eval(chunk)?;
                Ok(ArrayImpl::new_bool(
                    (0..array.len())
                        .map(|i| array.get(i) == DataValue::Null)
                        .collect(),
                ))
            }
            BoundExpr::ExprWithAlias(expr_with_alias) => expr_with_alias.expr.eval(chunk),
            _ => panic!("{:?} should not be evaluated in `eval_array`", self),
        }
    }

    /// Evaluate the given expression as an array in storage engine.
    pub fn eval_array_in_storage(
        &self,
        chunk: &PackedVec<Option<ArrayImpl>>,
        cardinality: usize,
    ) -> Result<ArrayImpl, ConvertError> {
        match &self {
            BoundExpr::InputRef(input_ref) => Ok(chunk[input_ref.index].clone().unwrap()),
            BoundExpr::BinaryOp(binary_op) => {
                let left = binary_op
                    .left_expr
                    .eval_array_in_storage(chunk, cardinality)?;
                let right = binary_op
                    .right_expr
                    .eval_array_in_storage(chunk, cardinality)?;
                left.binary_op(&binary_op.op, &right)
            }
            BoundExpr::UnaryOp(op) => {
                let array = op.expr.eval_array_in_storage(chunk, cardinality)?;
                array.unary_op(&op.op)
            }
            BoundExpr::Constant(v) => {
                let mut builder = ArrayBuilderImpl::with_capacity(cardinality, &self.return_type());
                // TODO: optimize this
                for _ in 0..cardinality {
                    builder.push(v);
                }
                Ok(builder.finish())
            }
            BoundExpr::TypeCast(cast) => {
                let array = cast.expr.eval_array_in_storage(chunk, cardinality)?;
                if self.return_type() == cast.expr.return_type() {
                    return Ok(array);
                }
                array.cast(&cast.ty)
            }
            BoundExpr::IsNull(expr) => {
                let array = expr.expr.eval_array_in_storage(chunk, cardinality)?;
                Ok(ArrayImpl::new_bool(
                    (0..array.len())
                        .map(|i| array.get(i) == DataValue::Null)
                        .collect(),
                ))
            }
            _ => panic!("{:?} should not be evaluated in `eval_array`", self),
        }
    }
}
