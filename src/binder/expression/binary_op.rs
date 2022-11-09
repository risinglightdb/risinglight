// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use serde::Serialize;

use super::*;
use crate::parser::BinaryOperator;
use crate::types::DataTypeKind;

/// A bound binary operation expression.
#[derive(PartialEq, Clone, Serialize)]
pub struct BoundBinaryOp {
    pub op: BinaryOperator,
    pub left_expr: Box<BoundExpr>,
    pub right_expr: Box<BoundExpr>,
    pub return_type: DataType,
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

        use crate::types::DataTypeKind::*;
        let mut left_bound_expr = self.bind_expr(left)?;
        let mut right_bound_expr = self.bind_expr(right)?;

        // Implicit type cast
        let left_data_type_kind = {
            let left_kind = left_bound_expr.return_type().kind();
            let right_kind = right_bound_expr.return_type().kind();
            let mut return_type_tmp = left_kind.clone();
            // Check if implicit type conversion is needed
            if left_kind != right_kind {
                // Insert type cast expr
                match (&left_kind, &right_kind) {
                    (Float64 | Decimal(_, _), Int32 | Int64)
                    | (Int64, Int32)
                    | (Date, String)
                    | (Decimal(_, _), Float64 | Decimal(None, None)) => {
                        right_bound_expr = BoundExpr::TypeCast(BoundTypeCast {
                            expr: Box::new(right_bound_expr),
                            ty: left_kind,
                        });
                    }
                    (Int32 | Int64, Float64 | Decimal(_, _))
                    | (Int32, Int64)
                    | (String, Date)
                    | (Float64 | Decimal(None, None), Decimal(_, _)) => {
                        left_bound_expr = BoundExpr::TypeCast(BoundTypeCast {
                            expr: Box::new(left_bound_expr),
                            ty: right_kind.clone(),
                        });
                        return_type_tmp = right_kind;
                    }
                    (Date, Interval) => {}
                    (left_kind, right_kind) => todo!(
                        "Support implicit conversion of {:?} and {:?}",
                        left_kind,
                        right_kind
                    ),
                }
            }
            return_type_tmp.nullable()
        };

        let return_type = match op {
            Op::Plus | Op::Minus | Op::Multiply | Op::Divide | Op::Modulo => left_data_type_kind,
            Op::Gt | Op::GtEq | Op::Lt | Op::LtEq | Op::Eq | Op::NotEq | Op::And | Op::Or => {
                DataTypeKind::Bool.nullable()
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
