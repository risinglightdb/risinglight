use crate::{
    array::*,
    binder::{BoundExpr, BoundExprKind},
    expr::{BinaryExpression, BinaryVectorizedExpression},
    parser::BinaryOperator,
    types::{DataTypeKind, DataValue},
};

impl BoundExpr {
    /// Evaluate the given expression.
    pub fn eval(&self) -> DataValue {
        match &self.kind {
            BoundExprKind::Constant(v) => v.clone(),
            _ => todo!("evaluate expression"),
        }
    }

    pub fn eval_array(&self, chunk: &DataChunk) -> Result<ArrayImpl, ConvertError> {
        match &self.kind {
            BoundExprKind::ColumnRef(col_ref) => {
                let mut builder = ArrayBuilderImpl::new(self.return_type.clone().unwrap());
                builder.append(chunk.array_at(col_ref.column_index as usize));
                Ok(builder.finish())
            }
            BoundExprKind::BinaryOp(binary_op) => {
                let left_arr = binary_op.left_expr.eval_array(chunk)?;
                let right_arr = binary_op.right_expr.eval_array(chunk)?;
                Ok(self.eval_binary_expr(left_arr, &binary_op.op, right_arr))
            }
            BoundExprKind::Constant(v) => {
                let mut builder = ArrayBuilderImpl::new(self.return_type.clone().unwrap());
                builder.push(v);
                Ok(builder.finish())
            }
            BoundExprKind::TypeCast(cast) => {
                let array = cast.expr.eval_array(chunk)?;
                if self.return_type == cast.expr.return_type {
                    return Ok(array);
                }
                array.try_cast(cast.ty.clone())
            }
            _ => todo!("evaluate expression: {:?}", self),
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
                (ArrayImpl::Float64(_), ArrayImpl::Float64(_)) => {
                    BinaryVectorizedExpression::<F64Array, F64Array, F64Array, _>::new(|x, y| {
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

impl ArrayImpl {
    /// Cast the array to another type.
    pub fn try_cast(&self, data_type: DataTypeKind) -> Result<Self, ConvertError> {
        Ok(match self {
            Self::Bool(a) => match data_type {
                DataTypeKind::Boolean => Self::Bool(a.iter().map(|o| o.cloned()).collect()),
                DataTypeKind::Int => Self::Int32(a.iter().map(|o| o.map(|&b| b as i32)).collect()),
                DataTypeKind::Float(_) | DataTypeKind::Double => {
                    Self::Float64(a.iter().map(|o| o.map(|&b| b as i32 as f64)).collect())
                }
                DataTypeKind::String => Self::UTF8(
                    a.iter()
                        .map(|o| o.map(|&b| if b { "true" } else { "false" }))
                        .collect(),
                ),
                _ => todo!("cast array"),
            },
            Self::Int32(a) => match data_type {
                DataTypeKind::Boolean => Self::Bool(a.iter().map(|o| o.map(|&i| i != 0)).collect()),
                DataTypeKind::Int => Self::Int32(a.iter().map(|o| o.cloned()).collect()),
                DataTypeKind::Float(_) | DataTypeKind::Double => {
                    Self::Float64(a.iter().map(|o| o.map(|&i| i as f64)).collect())
                }
                DataTypeKind::String => {
                    Self::UTF8(a.iter().map(|o| o.map(|i| i.to_string())).collect())
                }
                _ => todo!("cast array"),
            },
            Self::Float64(a) => match data_type {
                DataTypeKind::Boolean => {
                    Self::Bool(a.iter().map(|o| o.map(|&f| f != 0.0)).collect())
                }
                DataTypeKind::Int => Self::Int32(a.iter().map(|o| o.map(|&f| f as i32)).collect()),
                DataTypeKind::Float(_) | DataTypeKind::Double => {
                    Self::Float64(a.iter().map(|o| o.cloned()).collect())
                }
                DataTypeKind::String => {
                    Self::UTF8(a.iter().map(|o| o.map(|f| f.to_string())).collect())
                }
                _ => todo!("cast array"),
            },
            Self::UTF8(_a) => todo!("cast array"),
        })
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum ConvertError {
    #[error("failed to convert string to int")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("failed to convert string to float")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error("failed to convert string to bool")]
    ParseBool(#[from] std::str::ParseBoolError),
}
