use super::*;
use crate::types::DataTypeKind;
use sqlparser::ast::{Function, FunctionArg};

#[derive(Debug, PartialEq, Clone)]
pub struct BoundFunctionCall {
    pub op: String,
    pub args: Vec<BoundExpr>,
}

impl Binder {
    pub fn bind_function_call(&mut self, f: &Function) -> Result<BoundExpr, BindError> {
        let op = f.name.to_string();
        let args = f
            .args
            .iter()
            .map(|arg| match &arg {
                FunctionArg::Named { arg, .. } => self.bind_expr(arg).unwrap(),
                FunctionArg::Unnamed(arg) => self.bind_expr(arg).unwrap(),
            })
            .collect::<Vec<BoundExpr>>();

        // Return type depends on the aggregation type
        let return_type = match op.to_lowercase().as_str() {
            "sum" => Some(DataType::new(DataTypeKind::Int, false)),
            _ => panic!("{} is not supported", op),
        };

        Ok(BoundExpr {
            kind: BoundExprKind::FunctionCall(BoundFunctionCall {
                op: f.name.to_string(),
                args,
            }),
            return_type,
        })
    }
}
