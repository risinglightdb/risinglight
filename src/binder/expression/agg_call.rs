use super::*;
use crate::binder::{BindError, Binder, BoundExpr};
use crate::parser::{BinaryOperator, FunctionArg};
use crate::types::{DataType, DataTypeKind};

/// Aggregation kind
#[derive(Debug, PartialEq, Clone)]
pub enum AggKind {
    Avg,
    RowCount,
    Max,
    Min,
    Sum,
    // TODO: add Count
}

/// Represents an aggregation function
#[derive(Debug, PartialEq, Clone)]
pub struct BoundAggCall {
    pub kind: AggKind,
    pub args: Vec<BoundExpr>,
    pub return_type: DataType,
    // TODO: add distinct keyword
}

impl Binder {
    pub fn bind_function(&mut self, func: &Function) -> Result<BoundExpr, BindError> {
        // TODO: Support scalar function
        let mut args = Vec::new();
        for arg in &func.args {
            args.push(match &arg {
                FunctionArg::Named { arg, .. } => self.bind_expr(arg)?,
                FunctionArg::Unnamed(arg) => self.bind_expr(arg)?,
            });
        }
        let (kind, return_type) = match func.name.to_string().to_lowercase().as_str() {
            "avg" => (
                AggKind::Avg,
                Some(DataType::new(DataTypeKind::Double, false)),
            ),
            "count" => (
                AggKind::RowCount,
                Some(DataType::new(DataTypeKind::Int, false)),
            ),
            "max" => (AggKind::Max, args[0].return_type.clone()),
            "min" => (AggKind::Min, args[0].return_type.clone()),
            "sum" => (AggKind::Sum, args[0].return_type.clone()),
            _ => panic!("Unsupported function: {}", func.name),
        };

        match kind {
            // Rewrite `avg` into `sum / count`
            AggKind::Avg => Ok(BoundExpr {
                kind: BoundExprKind::BinaryOp(BoundBinaryOp {
                    left_expr: Box::new(BoundExpr {
                        kind: BoundExprKind::AggCall(BoundAggCall {
                            kind: AggKind::Sum,
                            args: args.clone(),
                            return_type: args[0].return_type.clone().unwrap(),
                        }),
                        return_type: args[0].return_type.clone(),
                    }),
                    op: BinaryOperator::Divide,
                    right_expr: Box::new(BoundExpr {
                        kind: BoundExprKind::AggCall(BoundAggCall {
                            kind: AggKind::RowCount,
                            args,
                            return_type: DataType::new(DataTypeKind::Int, false),
                        }),
                        return_type: Some(DataType::new(DataTypeKind::Int, false)),
                    }),
                }),
                return_type,
            }),
            _ => Ok(BoundExpr {
                kind: BoundExprKind::AggCall(BoundAggCall {
                    kind,
                    args,
                    return_type: return_type.clone().unwrap(),
                }),
                return_type,
            }),
        }
    }
}
