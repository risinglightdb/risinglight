// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use serde::Serialize;

use super::*;
use crate::parser::BinaryOperator;
use crate::types::{DataTypeExt, DataTypeKind};

/// A bound binary operation expression.
#[derive(PartialEq, Clone, Serialize)]
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

impl std::fmt::Display for BoundBinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} {} {})", self.left_expr, self.op, self.right_expr)
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

        use crate::types::PhysicalDataTypeKind::*;
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
                let mut return_type_tmp = left_data_type.kind();
                // Check if implicit type conversion is needed
                if left_physical_kind != right_physical_kind {
                    // Insert type cast expr
                    match (left_physical_kind, right_physical_kind) {
                        (Float64 | Decimal, Int32 | Int64)
                        | (Int64, Int32)
                        | (Date, String)
                        | (Decimal, Float64) => {
                            right_bound_expr = BoundExpr::TypeCast(BoundTypeCast {
                                expr: Box::new(right_bound_expr),
                                ty: left_data_type.kind(),
                            });
                        }
                        (Int32 | Int64, Float64 | Decimal)
                        | (Int32, Int64)
                        | (String, Date)
                        | (Float64, Decimal) => {
                            left_bound_expr = BoundExpr::TypeCast(BoundTypeCast {
                                expr: Box::new(left_bound_expr),
                                ty: right_data_type.kind(),
                            });
                            return_type_tmp = right_data_type.kind();
                        }
                        (Date, Interval) => {}
                        (left_kind, right_kind) => todo!(
                            "Support implicit conversion of {:?} and {:?}",
                            left_kind,
                            right_kind
                        ),
                    }
                }
                Some(return_type_tmp.nullable())
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
