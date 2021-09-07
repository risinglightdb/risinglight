use super::*;
use crate::catalog::ColumnRefId;
use crate::parser::{ExprKind, Expression};

mod column_ref;

impl Bind for Expression {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        self.kind.bind(binder)
    }
}

impl Bind for ExprKind {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        match self {
            // Binding constant is not necessary
            ExprKind::Constant(_) => Ok(()),
            ExprKind::ColumnRef(col_ref) => col_ref.bind(binder),
            _ => todo!(),
        }
    }
}
