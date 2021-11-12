use super::*;
use crate::parser::BinaryOperator;
use crate::types::{DataTypeKind, DataTypeExt};

/// A bound binary operation expression.
#[derive(PartialEq, Clone)]
pub struct BoundBinaryOp {
    pub left_expr: Box<BoundExpr>,
    pub op: BinaryOperator,
    pub right_expr: Box<BoundExpr>,
}

impl std::fmt::Debug for BoundBinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}({:?}, {:?})",
            self.op, self.left_expr, self.right_expr
        )
    }
}

impl Binder {
    pub fn bind_binary_op(
        &mut self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
    ) -> Result<BoundExpr, BindError> {
        let return_type;
        let left_bound_expr = self.bind_expr(left)?;
        let right_bound_expr = self.bind_expr(right)?;
        use BinaryOperator as Op;
        match op {
            Op::Plus
            | Op::Minus
            | Op::Multiply
            | Op::Divide
            | Op::Modulo
             => match (&left_bound_expr.return_type, &right_bound_expr.return_type) {
                (Some(left_data_type), Some(right_data_type)) => {
                    if left_data_type.kind() != right_data_type.kind() {
                        return Err(BindError::BinaryOpTypeMismatch(
                            format!("{:?}", left_data_type),
                            format!("{:?}", right_data_type),
                        ));
                    }
                    return_type = Some(left_data_type.kind().nullable());
                }
                (None, None) => return_type = None,
                _ => {
                    return Err(BindError::BinaryOpTypeMismatch(
                        "None".to_string(),
                        "None".to_string(),
                    ))
                }
            },
            Op::Gt
            | Op::GtEq
            | Op::Lt
            | Op::LtEq
            | Op::Eq
            | Op::NotEq
            | Op::And
            | Op::Or => {
                return_type = Some(DataTypeKind::Boolean.nullable());
            }
            _ => todo!("Support more binary operators"),
        }
        Ok(BoundExpr {
            kind: BoundExprKind::BinaryOp(BoundBinaryOp {
                left_expr: Box::new(left_bound_expr),
                op: op.clone(),
                right_expr: Box::new(right_bound_expr),
            }),
            return_type,
        })
    }
}
