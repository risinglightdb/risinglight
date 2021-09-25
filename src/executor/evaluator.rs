use crate::{
    array::{ArrayBuilderImpl, ArrayImpl, DataChunk, I32Array},
    binder::{BoundExpr, BoundExprKind},
    types::DataValue,
};

use crate::expr::{BinaryExpression, BinaryVectorizedExpression};
use crate::parser::BinaryOperator;

impl BoundExpr {
    /// Evaluate the given expression.
    pub fn eval(&self) -> DataValue {
        match &self.kind {
            BoundExprKind::Constant(v) => v.clone(),
            _ => todo!("evaluate expression"),
        }
    }

    pub fn eval_array(&self, chunk: &DataChunk) -> ArrayImpl {
        match &self.kind {
            BoundExprKind::ColumnRef(col_ref) => {
                let mut builder = ArrayBuilderImpl::new(self.return_type.clone().unwrap());
                builder.append(chunk.array_at(col_ref.column_index as usize));
                builder.finish()
            }
            BoundExprKind::BinaryOp(binary_op) => {
                let left_arr = binary_op.left_expr.eval_array(chunk);
                let right_arr = binary_op.right_expr.eval_array(chunk);
                self.eval_binary_expr(left_arr, &binary_op.op, right_arr)
            }
            BoundExprKind::Constant(v) => {
                let mut builder = ArrayBuilderImpl::new(self.return_type.clone().unwrap());
                builder.push(v);
                builder.finish()
            }
            _ => todo!("evaluate expression"),
        }
    }

    pub fn eval_binary_expr(
        &self,
        left_arr: ArrayImpl,
        op: &BinaryOperator,
        right_arr: ArrayImpl,
    ) -> ArrayImpl {
        match op {
            BinaryOperator::Plus => match (&left_arr, &right_arr) {
                (ArrayImpl::Int32(_), ArrayImpl::Int32(_)) => {
                    BinaryVectorizedExpression::<I32Array, I32Array, I32Array, _>::new(|x, y| {
                        x.and_then(|x| y.map(|y| x + y))
                    })
                    .eval_chunk(&left_arr, &right_arr)
                }
                _ => todo!("Support more types for plus!"),
            },
            _ => todo!("Support more operators"),
        }
    }
}
