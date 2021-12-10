use super::*;
use crate::{
    parser::BinaryOperator,
    types::{DataTypeExt, DataTypeKind},
};

/// A bound binary operation expression.
#[derive(PartialEq, Clone)]
pub struct BoundBinaryOp {
    pub op: BinaryOperator,
    pub left_expr: Box<BoundExpr>,
    pub right_expr: Box<BoundExpr>,
    pub return_type: Option<DataType>,
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
        use BinaryOperator as Op;
        let left_bound_expr = self.bind_expr(left)?;
        let right_bound_expr = self.bind_expr(right)?;
        let return_type = match op {
            Op::Plus | Op::Minus | Op::Multiply | Op::Divide | Op::Modulo => {
                match (
                    left_bound_expr.return_type(),
                    right_bound_expr.return_type(),
                ) {
                    (Some(left_data_type), Some(right_data_type)) => {
                        if left_data_type.kind() != right_data_type.kind() {
                            return Err(BindError::BinaryOpTypeMismatch(
                                format!("{:?}", left_data_type),
                                format!("{:?}", right_data_type),
                            ));
                        }
                        Some(left_data_type.kind().nullable())
                    }
                    (None, None) => None,
                    _ => {
                        return Err(BindError::BinaryOpTypeMismatch(
                            "None".to_string(),
                            "None".to_string(),
                        ))
                    }
                }
            }
            Op::Gt | Op::GtEq | Op::Lt | Op::LtEq | Op::Eq | Op::NotEq | Op::And | Op::Or => {
                Some(DataTypeKind::Boolean.nullable())
            }
            _ => todo!("Support more binary operators"),
        };
        Ok(BoundExpr::BinaryOp(BoundBinaryOp {
            op: op.clone(),
            left_expr: left_bound_expr.into(),
            right_expr: right_bound_expr.into(),
            return_type,
        }))
    }
}
