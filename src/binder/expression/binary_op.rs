use super::*;
use crate::parser::BinaryOperator;
use crate::types::{DataTypeExt, DataTypeKind, PhysicalDataTypeKind};

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
        let mut left_bound_expr = self.bind_expr(left)?;
        let mut right_bound_expr = self.bind_expr(right)?;

        // Implicit type cast
        let left_data_type_kind = match (
            left_bound_expr.return_type(),
            right_bound_expr.return_type(),
        ) {
            (Some(left_data_type), Some(right_data_type)) => {
                let left_physical_kind = left_data_type.physical_kind();
                let right_physical_kind = right_data_type.physical_kind();
                if left_physical_kind != right_physical_kind {
                    match (left_physical_kind, right_physical_kind) {
                        (
                            PhysicalDataTypeKind::Float64 | PhysicalDataTypeKind::Decimal,
                            PhysicalDataTypeKind::Int32 | PhysicalDataTypeKind::Int64,
                        )
                        | (PhysicalDataTypeKind::Date, PhysicalDataTypeKind::String) => {
                            right_bound_expr = BoundExpr::TypeCast(BoundTypeCast {
                                expr: Box::new(right_bound_expr),
                                ty: left_data_type.kind(),
                            });
                        }
                        (
                            PhysicalDataTypeKind::Int32 | PhysicalDataTypeKind::Int64,
                            PhysicalDataTypeKind::Float64 | PhysicalDataTypeKind::Decimal,
                        )
                        | (PhysicalDataTypeKind::String, PhysicalDataTypeKind::Date) => {
                            left_bound_expr = BoundExpr::TypeCast(BoundTypeCast {
                                expr: Box::new(left_bound_expr),
                                ty: right_data_type.kind(),
                            });
                        }
                        (left_kind, right_kind) => todo!(
                            "Support implicit conversion of {:?} and {:?}",
                            left_kind,
                            right_kind
                        ),
                    }
                }
                Some(left_data_type.kind().nullable())
            }
            (None, None) => None,
            (left, right) => {
                return Err(BindError::BinaryOpTypeMismatch(
                    format!("{:?}", left),
                    format!("{:?}", right),
                ))
            }
        };

        let return_type = match op {
            Op::Plus | Op::Minus | Op::Multiply | Op::Divide | Op::Modulo => left_data_type_kind,
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
