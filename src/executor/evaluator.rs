use crate::{parser::*, types::DataValue};

impl Expression {
    /// Evaluate the given expression.
    pub fn eval(&self) -> DataValue {
        match &self.kind {
            ExprKind::Constant(v) => v.clone(),
            _ => todo!("evaluate expression"),
        }
    }
}
