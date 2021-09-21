use super::*;

#[derive(Debug, PartialEq, Clone)]
pub struct BoundBinaryOp {
    pub left_expr: Box<BoundExpr>,
    pub op: BinaryOperator,
    pub right_bound_expr: Box<BoundExpr>,
}

impl Binder {
    pub fn bind_binary_op(
        &mut self,
        left: &Box<Expr>,
        op: &BinaryOperator,
        right: &Box<Expr>,
    ) -> Result<BoundExpr, BindError> {
        let mut return_type: Option<DataType> = None;
        let left_bound_expr = self.bind_expr(left)?;
        let right_bound_expr = self.bind_expr(right)?;
        match op {
            BinaryOperator::Plus => {
                match (&left_bound_expr.return_type, &right_bound_expr.return_type) {
                    (Some(left_data_type), Some(right_data_type)) => {
                        return_type = Some(left_data_type.clone())
                    }
                    (None, None) => return_type = None,
                    _ => return Err(BindError::BinaryOpTypeMismatch),
                }
            }
            _ => todo!("Support more binary operators"),
        }
        Ok(BoundExpr {
            kind: BoundExprKind::BinaryOp(BoundBinaryOp {
                left_expr: Box::new(left_bound_expr),
                op: op.clone(),
                right_bound_expr: Box::new(right_bound_expr),
            }),
            return_type,
        })
    }
}
