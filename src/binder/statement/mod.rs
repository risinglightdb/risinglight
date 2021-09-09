use super::*;

use crate::parser::SQLStatement;

pub mod create;
pub mod insert;
pub mod select;

pub use create::*;
pub use insert::*;
pub use select::*;

impl Bind for SQLStatement {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        match self {
            Self::CreateDatabase(stmt) => todo!(),
            Self::CreateSchema(stmt) => todo!(),
            Self::CreateTable(stmt) => stmt.bind(binder),
            Self::Insert(stmt) => stmt.bind(binder),
            Self::Select(stmt) => stmt.bind(binder),
        }
    }
}
