use super::*;
use crate::catalog::ColumnRefId;
use crate::parser::{ExprKind, Expression};

mod column_ref;

impl Bind for Expression {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        match &mut self.kind {
            // Binding constant is not necessary
            ExprKind::Constant(_) => Ok(()),
            ExprKind::ColumnRef(col_ref) => {
                self.return_type = Some(col_ref.bind(binder)?);
                Ok(())
            }
            _ => todo!(),
        }
    }
}
