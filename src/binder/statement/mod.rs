use super::*;
use crate::parser::SQLStatement;

mod create;
mod insert;
mod select;

impl Bind for SQLStatement {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        match self {
            Self::CreateTable(stmt) => stmt.bind(binder),
            Self::Insert(stmt) => stmt.bind(binder),
            Self::Select(stmt) => todo!(),
        }
    }
}
