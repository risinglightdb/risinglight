// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt::Formatter;

use itertools::Itertools;
use serde::Serialize;

use super::*;
use crate::parser::{BinaryOperator, FunctionArg, FunctionArgExpr};
use crate::types::{DataType, DataTypeKind};

/// Aggregation kind
#[derive(Debug, PartialEq, Eq, Clone, Serialize)]
pub enum AggKind {
    Avg,
    RowCount,
    Max,
    Min,
    Sum,
    Count,
    First,
    Last,
}

impl std::fmt::Display for AggKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use AggKind::*;
        write!(
            f,
            "{}",
            match self {
                Avg => "avg",
                RowCount | Count => "count",
                Max => "max",
                Min => "min",
                Sum => "sum",
                First => "first",
                Last => "last",
            }
        )
    }
}

/// Represents an aggregation function
#[derive(PartialEq, Clone, Serialize)]
pub struct BoundAggCall {
    pub kind: AggKind,
    pub args: Vec<BoundExpr>,
    pub return_type: DataType,
    // TODO: add distinct keyword
}

impl std::fmt::Debug for BoundAggCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}({:?}) -> {:?}",
            self.kind, self.args, self.return_type
        )
    }
}

impl std::fmt::Display for BoundAggCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({}) -> {}",
            self.kind,
            self.args.iter().map(|x| format!("{}", x)).join(", "),
            self.return_type
        )
    }
}

impl Binder {
    pub fn bind_function(&mut self, func: &Function) -> Result<BoundExpr, BindError> {
        // TODO: Support scalar function
        let mut args = Vec::new();
        for arg in &func.args {
            let arg = match &arg {
                FunctionArg::Named { arg, .. } => arg,
                FunctionArg::Unnamed(arg) => arg,
            };
            match arg {
                FunctionArgExpr::Expr(expr) => args.push(self.bind_expr(expr)?),
                FunctionArgExpr::Wildcard => {
                    // No argument in row count
                    args.clear();
                    break;
                }
                _ => todo!("Support aggregate argument: {:?}", arg),
            }
        }

        let (kind, return_type) = match func.name.to_string().to_lowercase().as_str() {
            "avg" => (AggKind::Avg, args[0].return_type()),
            "count" => {
                if args.is_empty() {
                    let first_index_column = BoundExpr::InputRef(BoundInputRef {
                        index: 0,
                        return_type: DataTypeKind::Int32.not_null(),
                    });
                    args.push(first_index_column);
                    (AggKind::RowCount, DataTypeKind::Int32.not_null())
                } else {
                    (AggKind::Count, DataTypeKind::Int32.not_null())
                }
            }
            "max" => (AggKind::Max, args[0].return_type()),
            "min" => (AggKind::Min, args[0].return_type()),
            "sum" => (AggKind::Sum, args[0].return_type()),
            "first" => (AggKind::First, args[0].return_type()),
            "last" => (AggKind::Last, args[0].return_type()),
            _ => panic!("Unsupported function: {}", func.name),
        };

        match kind {
            // Rewrite `avg` into `sum / count`
            AggKind::Avg => Ok(BoundExpr::BinaryOp(BoundBinaryOp {
                op: BinaryOperator::Divide,
                left_expr: Box::new(BoundExpr::AggCall(BoundAggCall {
                    kind: AggKind::Sum,
                    args: args.clone(),
                    return_type: args[0].return_type(),
                })),
                right_expr: Box::new(BoundExpr::TypeCast(BoundTypeCast {
                    ty: args[0].return_type().kind(),
                    expr: Box::new(BoundExpr::AggCall(BoundAggCall {
                        kind: AggKind::Count,
                        args,
                        return_type: DataTypeKind::Int32.not_null(),
                    })),
                })),
                return_type,
            })),
            _ => Ok(BoundExpr::AggCall(BoundAggCall {
                kind,
                args,
                return_type,
            })),
        }
    }
}
